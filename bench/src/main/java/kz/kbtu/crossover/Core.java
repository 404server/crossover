package kz.kbtu.crossover;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The business logic shared by every configuration (the "core library"). A deployment only
 * decides which modules run in-process and which are reached over HTTP; the code is identical,
 * so any performance difference between configurations comes from the topology alone.
 */
public final class Core {
    private Core() {}

    static final int USERS = 10_000, PRODUCTS = 10_000, CATEGORIES = 50, PAGE = 20, HOT = 10;

    public record Product(long id, String name, String description, int category, long priceCents) {}
    public record Listing(Product product, int stock) {}
    public record Item(long productId, int qty) {}
    public record Profile(long id, String name, int tier) {}
    public record Receipt(long user, int items, long totalCents, String status) {}

    public interface Auth { long verify(String token); }
    public interface Profiles { Profile get(long userId); }
    public interface Search { List<Listing> browse(String token, int category, int page); }
    public interface Products { List<Product> get(List<Long> ids); }
    public interface Stock { Map<Long, Integer> levels(List<Long> ids); boolean take(List<Item> items); }
    public interface Reservations { long reserve(long userId, List<Item> items); }
    public interface Carts { List<Item> get(long userId); }
    public interface Orders { Receipt checkout(String token); }

    /** Module registry. Each field holds either a local implementation or an HTTP stub. */
    public static final class Modules {
        Auth auth; Profiles profiles; Search search; Products products;
        Stock stock; Reservations reservations; Carts carts; Orders orders;
        /** Locally hosted implementations, keyed by interface simple name (the RPC route). */
        final Map<String, Object> local = new HashMap<>();

        <T> T pick(Class<T> type, Set<String> hosted, Function<Class<?>, String> url, Supplier<T> impl) {
            if (!hosted.contains(type.getSimpleName())) return Rpc.stub(type, url.apply(type));
            T t = WORK_NS > 0 ? withWork(type, impl.get()) : impl.get();
            local.put(type.getSimpleName(), t);
            return t;
        }
    }

    /** Synthetic service demand: WORK_US microseconds of CPU in every module operation, wherever it is hosted. */
    static final long WORK_NS = 1000L * Long.parseLong(System.getenv().getOrDefault("WORK_US", "0"));
    static final ThreadMXBean THREADS = ManagementFactory.getThreadMXBean();

    @SuppressWarnings("unchecked")
    static <T> T withWork(Class<T> type, T target) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            burn(WORK_NS);
            try { return method.invoke(target, args); }
            catch (InvocationTargetException e) { throw e.getCause(); }
        });
    }

    /** Spins until this thread has used ns of CPU time, so time spent throttled or descheduled does not count. */
    static void burn(long ns) {
        long end = THREADS.getCurrentThreadCpuTime() + ns;
        while (THREADS.getCurrentThreadCpuTime() < end) Thread.onSpinWait();
    }

    static Modules wire(Set<String> hosted, JdbcTemplate db, Function<Class<?>, String> url) {
        Modules m = new Modules();
        m.auth = m.pick(Auth.class, hosted, url, () -> new AuthImpl(db));
        m.profiles = m.pick(Profiles.class, hosted, url, () -> new ProfilesImpl(db));
        m.search = m.pick(Search.class, hosted, url, () -> new SearchImpl(db, m));
        m.products = m.pick(Products.class, hosted, url, () -> new ProductsImpl(db));
        m.stock = m.pick(Stock.class, hosted, url, () -> new StockImpl(db));
        m.reservations = m.pick(Reservations.class, hosted, url, () -> new ReservationsImpl(db, m));
        m.carts = m.pick(Carts.class, hosted, url, () -> new CartsImpl(db));
        m.orders = m.pick(Orders.class, hosted, url, () -> new OrdersImpl(db, m));
        return m;
    }

    // ---------------------------------------------------------------- implementations

    record AuthImpl(JdbcTemplate db) implements Auth {
        AuthImpl {
            seed(db, "sessions", "CREATE TABLE IF NOT EXISTS sessions (token text PRIMARY KEY, user_id bigint NOT NULL)",
                "INSERT INTO sessions VALUES (?, ?)", USERS, (ps, i) -> { ps.setString(1, "t" + i); ps.setLong(2, i); });
        }
        public long verify(String token) {
            List<Long> r = Timing.db(() -> db.queryForList("SELECT user_id FROM sessions WHERE token = ?", Long.class, token));
            if (r.isEmpty()) throw new IllegalArgumentException("unknown session");
            return r.getFirst();
        }
    }

    record ProfilesImpl(JdbcTemplate db) implements Profiles {
        ProfilesImpl {
            seed(db, "users", "CREATE TABLE IF NOT EXISTS users (id bigint PRIMARY KEY, name text NOT NULL, tier int NOT NULL)",
                "INSERT INTO users VALUES (?, ?, ?)", USERS, (ps, i) -> { ps.setLong(1, i); ps.setString(2, "user-" + i); ps.setInt(3, i % 3); });
        }
        public Profile get(long userId) {
            return Timing.db(() -> db.queryForObject("SELECT id, name, tier FROM users WHERE id = ?",
                (rs, n) -> new Profile(rs.getLong(1), rs.getString(2), rs.getInt(3)), userId));
        }
    }

    /** Owns the category index and the browse use case (the read-heavy edge operation). */
    record SearchImpl(JdbcTemplate db, Modules m) implements Search {
        SearchImpl {
            seed(db, "product_index", "CREATE TABLE IF NOT EXISTS product_index (category int NOT NULL, popularity int NOT NULL, "
                    + "product_id bigint NOT NULL, PRIMARY KEY (category, popularity, product_id))",
                "INSERT INTO product_index VALUES (?, ?, ?)", PRODUCTS,
                (ps, i) -> { ps.setInt(1, i % CATEGORIES); ps.setInt(2, (i * 7919) % 1000); ps.setLong(3, i); });
        }
        public List<Listing> browse(String token, int category, int page) {
            m.auth.verify(token);
            List<Long> ids = Timing.db(() -> db.queryForList("SELECT product_id FROM product_index WHERE category = ? "
                + "ORDER BY popularity DESC, product_id LIMIT ? OFFSET ?", Long.class, category, PAGE, page * PAGE));
            List<Product> products = m.products.get(ids);
            Map<Long, Integer> levels = m.stock.levels(ids);
            List<Listing> out = new ArrayList<>(products.size());
            for (Product p : products) out.add(new Listing(p, levels.getOrDefault(p.id(), 0)));
            return out;
        }
    }

    record ProductsImpl(JdbcTemplate db) implements Products {
        static final String FILLER = " Durable, well reviewed and shipped from the regional warehouse within two working days;"
            + " covered by the standard return policy and a one-year limited warranty.";
        ProductsImpl {
            seed(db, "products", "CREATE TABLE IF NOT EXISTS products (id bigint PRIMARY KEY, name text NOT NULL, "
                    + "description text NOT NULL, category int NOT NULL, price_cents bigint NOT NULL)",
                "INSERT INTO products VALUES (?, ?, ?, ?, ?)", PRODUCTS, (ps, i) -> {
                    ps.setLong(1, i); ps.setString(2, "product-" + i); ps.setString(3, "Product " + i + "." + FILLER);
                    ps.setInt(4, i % CATEGORIES); ps.setLong(5, 199 + (i * 37L) % 9800); });
        }
        public List<Product> get(List<Long> ids) {
            if (ids.isEmpty()) return List.of();
            Map<Long, Product> byId = new HashMap<>();
            Timing.db(() -> db.query("SELECT id, name, description, category, price_cents FROM products WHERE id = ANY (?::bigint[])",
                rs -> { byId.put(rs.getLong(1), new Product(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getLong(5))); },
                pgArray(ids)));
            List<Product> out = new ArrayList<>(ids.size());
            for (Long id : ids) if (byId.containsKey(id)) out.add(byId.get(id));
            return out;
        }
    }

    record StockImpl(JdbcTemplate db) implements Stock {
        StockImpl {
            seed(db, "stock", "CREATE TABLE IF NOT EXISTS stock (product_id bigint PRIMARY KEY, qty bigint NOT NULL)",
                "INSERT INTO stock VALUES (?, ?)", PRODUCTS, (ps, i) -> { ps.setLong(1, i); ps.setLong(2, 1_000_000_000_000L); });
        }
        public Map<Long, Integer> levels(List<Long> ids) {
            if (ids.isEmpty()) return Map.of();
            Map<Long, Integer> out = new LinkedHashMap<>();
            Timing.db(() -> db.query("SELECT product_id, least(qty, 99) FROM stock WHERE product_id = ANY (?::bigint[])",
                rs -> { out.put(rs.getLong(1), rs.getInt(2)); }, pgArray(ids)));
            return out;
        }
        /** Decrements all items atomically in one statement; hot items make this the contention point. */
        public boolean take(List<Item> items) {
            StringBuilder sql = new StringBuilder("UPDATE stock s SET qty = s.qty - v.q FROM (VALUES ");
            List<Object> args = new ArrayList<>();
            for (int i = 0; i < items.size(); i++) {
                sql.append(i == 0 ? "" : ", ").append("(?::bigint, ?::int)");
                args.add(items.get(i).productId()); args.add(items.get(i).qty());
            }
            sql.append(") AS v(id, q) WHERE s.product_id = v.id AND s.qty >= v.q");
            return Timing.db(() -> db.update(sql.toString(), args.toArray())) == items.size();
        }
    }

    record ReservationsImpl(JdbcTemplate db, Modules m) implements Reservations {
        ReservationsImpl {
            seed(db, "reservations", "CREATE TABLE IF NOT EXISTS reservations (id bigserial PRIMARY KEY, user_id bigint NOT NULL, "
                + "items int NOT NULL, created timestamptz NOT NULL DEFAULT now())", null, 0, null);
        }
        public long reserve(long userId, List<Item> items) {
            if (!m.stock.take(items)) throw new IllegalStateException("out of stock");
            return Timing.db(() -> db.queryForObject("INSERT INTO reservations (user_id, items) VALUES (?, ?) RETURNING id",
                Long.class, userId, items.size()));
        }
    }

    /** Every cart holds one of HOT popular products plus two distinct regular ones. */
    record CartsImpl(JdbcTemplate db) implements Carts {
        CartsImpl {
            seed(db, "cart_items", "CREATE TABLE IF NOT EXISTS cart_items (user_id bigint NOT NULL, product_id bigint NOT NULL, "
                    + "qty int NOT NULL, PRIMARY KEY (user_id, product_id))",
                "INSERT INTO cart_items VALUES (?, ?, ?)", USERS * 3, (ps, i) -> {
                    int u = i / 3, k = i % 3;
                    long product = k == 0 ? u % HOT : HOT + (2L * u + k - 1) % (PRODUCTS - HOT);
                    ps.setLong(1, u); ps.setLong(2, product); ps.setInt(3, 1 + (u + k) % 2); });
        }
        public List<Item> get(long userId) {
            return Timing.db(() -> db.query("SELECT product_id, qty FROM cart_items WHERE user_id = ? ORDER BY product_id",
                (rs, n) -> new Item(rs.getLong(1), rs.getInt(2)), userId));
        }
    }

    /** Owns the checkout use case (the write-heavy, deep edge operation). */
    record OrdersImpl(JdbcTemplate db, Modules m) implements Orders {
        OrdersImpl {
            seed(db, "orders", "CREATE TABLE IF NOT EXISTS orders (id bigserial PRIMARY KEY, user_id bigint NOT NULL, "
                + "total_cents bigint NOT NULL, reservation_id bigint NOT NULL, created timestamptz NOT NULL DEFAULT now())", null, 0, null);
        }
        public Receipt checkout(String token) {
            long user = m.auth.verify(token);
            Profile profile = m.profiles.get(user);
            List<Item> items = m.carts.get(user);
            Map<Long, Long> price = new HashMap<>();
            for (Product p : m.products.get(items.stream().map(Item::productId).toList())) price.put(p.id(), p.priceCents());
            long gross = 0;
            for (Item it : items) gross += price.get(it.productId()) * it.qty();
            long total = gross * (100 - 5L * profile.tier()) / 100;
            long reservation = m.reservations.reserve(user, items);
            Timing.db(() -> db.update("INSERT INTO orders (user_id, total_cents, reservation_id) VALUES (?, ?, ?)", user, total, reservation));
            return new Receipt(user, items.size(), total, "CONFIRMED");
        }
    }

    // ---------------------------------------------------------------- helpers

    interface Row { void bind(PreparedStatement ps, int i) throws Exception; }

    /** Creates a module's table and seeds it once; the advisory lock makes concurrent replicas safe. */
    static void seed(JdbcTemplate db, String table, String ddl, String insert, int rows, Row row) {
        db.execute((ConnectionCallback<Void>) c -> {
            try (Statement s = c.createStatement()) {
                s.execute("SELECT pg_advisory_lock(7342)");
                try {
                    s.execute(ddl);
                    if (rows > 0 && isEmpty(s, table)) insert(c, insert, rows, row);
                } finally {
                    s.execute("SELECT pg_advisory_unlock(7342)");
                }
            }
            return null;
        });
    }

    private static boolean isEmpty(Statement s, String table) throws java.sql.SQLException {
        try (ResultSet rs = s.executeQuery("SELECT NOT EXISTS (SELECT 1 FROM " + table + ")")) { rs.next(); return rs.getBoolean(1); }
    }

    private static void insert(Connection c, String sql, int rows, Row row) throws java.sql.SQLException {
        boolean auto = c.getAutoCommit();
        c.setAutoCommit(false);
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < rows; i++) {
                try { row.bind(ps, i); } catch (Exception e) { throw new java.sql.SQLException(e); }
                ps.addBatch();
                if (i % 1000 == 999) ps.executeBatch();
            }
            ps.executeBatch();
            c.commit();
        } finally {
            c.setAutoCommit(auto);
        }
    }

    /** Postgres array literal, e.g. {1,2,3}; avoids per-driver array plumbing. */
    static String pgArray(List<Long> ids) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < ids.size(); i++) sb.append(i == 0 ? "" : ",").append(ids.get(i));
        return sb.append('}').toString();
    }

    static List<String> moduleNames() {
        List<String> l = new ArrayList<>();
        for (Class<?> c : List.of(Auth.class, Profiles.class, Search.class, Products.class, Stock.class, Reservations.class, Carts.class, Orders.class))
            l.add(c.getSimpleName());
        return Collections.unmodifiableList(l);
    }
}
