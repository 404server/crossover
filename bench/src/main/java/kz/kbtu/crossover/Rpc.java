package kz.kbtu.crossover;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** JSON-over-HTTP/1.1 calls between modules hosted in different processes. */
final class Rpc {
    private Rpc() {}

    static final JsonMapper JSON = JsonMapper.builder().build();
    static final HttpClient HTTP = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    /** A client stub: every interface call becomes POST {base}/rpc/{Interface}/{method} with a JSON array of args. */
    static <T> T stub(Class<T> type, String baseUrl) {
        String prefix = baseUrl + "/rpc/" + type.getSimpleName() + "/";
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) return method.getName().equals("toString") ? "stub:" + prefix : null;
            long t0 = System.nanoTime();
            byte[] body = JSON.writeValueAsBytes(args == null ? new Object[0] : args);
            long t1 = System.nanoTime();
            HttpResponse<byte[]> r = HTTP.send(HttpRequest.newBuilder(URI.create(prefix + method.getName()))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(), HttpResponse.BodyHandlers.ofByteArray());
            long t2 = System.nanoTime();
            if (r.statusCode() != 200) throw new IllegalStateException(prefix + method.getName() + " -> HTTP " + r.statusCode());
            Object result = JSON.readValue(r.body(), JSON.getTypeFactory().constructType(method.getGenericReturnType()));
            long t3 = System.nanoTime();
            Timing t = Timing.CURRENT.get();
            if (t != null) t.call((t1 - t0) + (t3 - t2), t2 - t1, r.headers().firstValue("Server-Timing").orElse(""));
            return result;
        }));
    }

    static Object[] decodeArgs(byte[] body, Method m) {
        JsonNode arr = JSON.readTree(body);
        Type[] types = m.getGenericParameterTypes();
        Object[] args = new Object[types.length];
        for (int i = 0; i < types.length; i++) args[i] = JSON.treeToValue(arr.get(i), JSON.getTypeFactory().constructType(types[i]));
        return args;
    }
}

/**
 * Per-request latency accounting, propagated upstream in the W3C Server-Timing header.
 * Every layer is a measured interval; summed over the call tree they add up to the edge
 * process's total exactly: tot = app + db + ser + net + oth.
 * <ul>
 *   <li>app: time in module code, excluding db, serialisation and outbound calls</li>
 *   <li>db: JDBC round trips, including connection-pool wait</li>
 *   <li>ser: JSON encoding/decoding on both sides of every hop</li>
 *   <li>net: per hop, client-observed round trip minus the callee's own total (transport,
 *       HTTP stacks, kube-proxy, sidecars). Needs no clock synchronisation.</li>
 *   <li>oth: servlet/framework dispatch before the handler</li>
 * </ul>
 */
final class Timing {
    static final ThreadLocal<Timing> CURRENT = new ThreadLocal<>();

    final long start = System.nanoTime();
    long db, ser, outbound;                      // own time; outbound = wall time of calls incl. their serialisation
    long cApp, cDb, cSer, cNet, cOth, calls;     // aggregated from callees

    static <T> T db(java.util.function.Supplier<T> s) {
        long t0 = System.nanoTime();
        try { return s.get(); } finally { addDb(System.nanoTime() - t0); }
    }

    static void db(Runnable r) {
        long t0 = System.nanoTime();
        try { r.run(); } finally { addDb(System.nanoTime() - t0); }
    }

    private static void addDb(long ns) {
        Timing t = CURRENT.get();
        if (t != null) t.db += ns;
    }

    void call(long serNs, long rttNs, String serverTiming) {
        ser += serNs;
        outbound += serNs + rttNs;
        calls++;
        long[] v = parse(serverTiming);          // app, db, ser, net, oth, tot, calls  (ns)
        cApp += v[0]; cDb += v[1]; cSer += v[2]; cNet += v[3] + (rttNs - v[5]); cOth += v[4]; calls += v[6];
    }

    /** c0: handler entry, h0/h1: business-logic invocation, r1: response encoded. */
    String header(long c0, long h0, long h1, long r1) {
        long app = (h1 - h0) - db - outbound;
        return "app;dur=" + ms(app + cApp) + ", db;dur=" + ms(db + cDb) + ", ser;dur=" + ms(ser + cSer)
            + ", net;dur=" + ms(cNet) + ", oth;dur=" + ms((c0 - start) + cOth) + ", tot;dur=" + ms(r1 - start)
            + ", calls;dur=" + calls;
    }

    private static final String[] KEYS = {"app", "db", "ser", "net", "oth", "tot", "calls"};

    static long[] parse(String h) {
        long[] v = new long[KEYS.length];
        for (String part : h.split(",")) {
            int semi = part.indexOf(";dur=");
            if (semi < 0) continue;
            String key = part.substring(0, semi).trim();
            double val = Double.parseDouble(part.substring(semi + 5).trim());
            for (int i = 0; i < KEYS.length; i++)
                if (KEYS[i].equals(key)) v[i] = key.equals("calls") ? (long) val : Math.round(val * 1_000_000);
        }
        return v;
    }

    /** Nanoseconds as milliseconds with microsecond resolution. */
    static String ms(long ns) {
        long us = Math.round(ns / 1000.0);
        long abs = Math.abs(us);
        String frac = String.valueOf(1000 + abs % 1000).substring(1);
        return (us < 0 ? "-" : "") + (abs / 1000) + "." + frac;
    }
}
