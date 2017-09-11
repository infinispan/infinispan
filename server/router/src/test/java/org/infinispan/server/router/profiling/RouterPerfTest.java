package org.infinispan.server.router.profiling;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.test.categories.Profiling;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.MultiTenantRouter;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.RouteSource;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;

/**
 * This class is responsible for performance tests against the router. It tests 3 configurations: <ul> <li>No SSL at
 * all</li> <li>HotRod with SSL only</li> <li>Multi tenant router with SSL+SNI</li> </ul>
 * <p>
 * <p> Note that this class is not triggered by Surefire by default (it doesn't end with "test"). We want to do
 * performance test on demand only. </p>
 */
public class RouterPerfTest {

    private static final int MEASUREMENT_ITERATIONS_COUNT = 1;
    private static final int WARMUP_ITERATIONS_COUNT = 1;

    @Test
    @Category(Profiling.class)
    public void performRouterBenchmark() throws Exception {
        Options opt = new OptionsBuilder()
                .include(this.getClass().getName() + ".*")
                .mode(Mode.AverageTime)
                .mode(Mode.SingleShotTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .warmupIterations(WARMUP_ITERATIONS_COUNT)
                .measurementIterations(MEASUREMENT_ITERATIONS_COUNT)
                .threads(1)
                .forks(1)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Thread)
    public static class BenchmarkState {

        @Param({
                "org.infinispan.server.router.performance.configuration.SingleServerNoSsl",
                "org.infinispan.server.router.performance.configuration.SingleServerWithSsl",
                "org.infinispan.server.router.performance.configuration.TwoServersWithSslSni"
        })
        public String configurationClassName;

        private List<HotRodServer> hotRodServers;
        private Optional<Set<Route<? extends RouteSource, ? extends RouteDestination>>> routes;
        private RemoteCacheManager preloadedClient;
        private Optional<MultiTenantRouter> router;
        private PerfTestConfiguration configuration;

        @Setup
        public void setup() throws Exception {
            //Netty uses SLF and SLF can redirect to all other logging frameworks.
            //Just to make sure we know what we are testing against - let's enforce one of them
            InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
            //Temporary to make the test equal
            System.setProperty("infinispan.server.channel.epoll", "false");
            configuration = (PerfTestConfiguration) Class.forName(configurationClassName).getDeclaredConstructor(null).newInstance(null);
            hotRodServers = configuration.initServers();
            routes = configuration.initRoutes(hotRodServers);
            router = configuration.initRouter(routes);
            preloadedClient = configuration.initClient(router, routes, hotRodServers);
        }

        @TearDown
        public void tearDown() {
            preloadedClient.stop();
            configuration.shutdown(hotRodServers, router);
        }

        @Benchmark
        public void initConnectionOnly() {
            RemoteCacheManager client = configuration.initClient(router, routes, hotRodServers);
            client.stop();
        }

        @Benchmark
        public void initConnectionAndPerform10Puts() {
            RemoteCacheManager client = configuration.initClient(router, routes, hotRodServers);
            configuration.performLoadTesting(client, 10);
            client.stop();
        }

        @Benchmark
        public void initConnectionAndPerform10KPuts() {
            RemoteCacheManager client = configuration.initClient(router, routes, hotRodServers);
            configuration.performLoadTesting(client, 10_000);
            client.stop();
        }

        @Benchmark
        public void perform10KPuts() {
            configuration.performLoadTesting(preloadedClient, 10_000);
        }
    }

}
