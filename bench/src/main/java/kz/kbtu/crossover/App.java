package kz.kbtu.crossover;

import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletResponse;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * One artifact for every configuration. SERVICE_MODULES lists the modules this process hosts
 * (default: all, i.e. the monolith); every other module is reached at URL_&lt;Module&gt;.
 */
@SpringBootApplication
@RestController
public class App {
    public static void main(String[] args) { SpringApplication.run(App.class, args); }

    private final Core.Modules mods;
    private final Map<String, Method> methods = new HashMap<>();

    App(JdbcTemplate db, Environment env) {
        Set<String> hosted = Set.of(env.getProperty("SERVICE_MODULES", String.join(",", Core.moduleNames())).split(","));
        mods = Core.wire(hosted, db, type -> env.getRequiredProperty("URL_" + type.getSimpleName()));
        for (Object impl : mods.local.values())
            for (Class<?> iface : impl.getClass().getInterfaces())
                for (Method m : iface.getMethods()) methods.put(iface.getSimpleName() + "/" + m.getName(), m);
    }

    @Bean
    FilterRegistrationBean<Filter> timingFilter() {
        FilterRegistrationBean<Filter> f = new FilterRegistrationBean<>((req, res, chain) -> {
            Timing.CURRENT.set(new Timing());
            try { chain.doFilter(req, res); } finally { Timing.CURRENT.remove(); }
        });
        f.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return f;
    }

    @GetMapping("/health")
    String health() { return "ok " + mods.local.keySet(); }

    @GetMapping("/api/browse")
    void browse(@RequestParam int category, @RequestParam(defaultValue = "0") int page,
                @RequestHeader("Authorization") String auth, HttpServletResponse res) throws Exception {
        edge(res, "Search", () -> mods.search.browse(token(auth), category, page));
    }

    @PostMapping("/api/checkout")
    void checkout(@RequestHeader("Authorization") String auth, HttpServletResponse res) throws Exception {
        edge(res, "Orders", () -> mods.orders.checkout(token(auth)));
    }

    @PostMapping("/rpc/{module}/{method}")
    void rpc(@PathVariable String module, @PathVariable String method, @RequestBody byte[] body,
             HttpServletResponse res) throws Exception {
        long c0 = System.nanoTime();
        Method m = methods.get(module + "/" + method);
        if (m == null) { res.sendError(404, module + "/" + method + " is not hosted here"); return; }
        Object[] args = Rpc.decodeArgs(body, m);
        Timing t = Timing.CURRENT.get();
        long h0 = System.nanoTime();
        t.ser += h0 - c0;
        Object result;
        try { result = m.invoke(mods.local.get(module), args); }
        catch (InvocationTargetException e) { throw e.getCause() instanceof Exception x ? x : e; }
        respond(res, t, c0, h0, System.nanoTime(), result);
    }

    /** Edge operations are only served by the process that hosts the owning module, never proxied. */
    private void edge(HttpServletResponse res, String owner, Callable<Object> op) throws Exception {
        if (!mods.local.containsKey(owner)) { res.sendError(404, owner + " is not hosted here"); return; }
        Timing t = Timing.CURRENT.get();
        long c0 = System.nanoTime();
        Object result = op.call();
        respond(res, t, c0, c0, System.nanoTime(), result);
    }

    private static void respond(HttpServletResponse res, Timing t, long c0, long h0, long h1, Object result) throws Exception {
        byte[] out = Rpc.JSON.writeValueAsBytes(result);
        long r1 = System.nanoTime();
        t.ser += r1 - h1;
        res.setHeader("Server-Timing", t.header(c0, h0, h1, r1));
        res.setContentType("application/json");
        res.setContentLength(out.length);
        res.getOutputStream().write(out);
    }

    private static String token(String authorization) {
        return authorization.startsWith("Bearer ") ? authorization.substring(7) : authorization;
    }
}
