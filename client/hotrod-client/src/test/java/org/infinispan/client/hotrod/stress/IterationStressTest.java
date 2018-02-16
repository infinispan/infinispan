package org.infinispan.client.hotrod.stress;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Simple stress test for remote iteration of entries.
 * By default, it runs the server and the client in the same VM; when the sys prop -DserverHost=server
 * is present it will connect to that server instead.
 */
@Test(groups = "stress", testName = "org.infinispan.client.hotrod.stress.IterationStressTest")
public class IterationStressTest extends SingleHotRodServerTest {

    private static final int NUM_ENTRIES = 500_000;
    private static final String SERVER_HOST = "serverHost";
    public static final int THREADS = Runtime.getRuntime().availableProcessors();

    private RemoteCache<Object, Object> remoteCache;

    @Override
    protected void setup() throws Exception {
        String serverHost = System.getProperty(SERVER_HOST);
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder =
                new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();

        if (serverHost == null) {
            cacheManager = createCacheManager();
            hotrodServer = createHotRodServer();
            builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
        } else {
            builder.addServer().host(serverHost);
        }

        remoteCacheManager = new RemoteCacheManager(builder.build());
        remoteCacheManager.getCache();

        remoteCache = remoteCacheManager.getCache();

        AtomicInteger counter = new AtomicInteger();
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        CompletableFuture[] futures = new CompletableFuture[THREADS];

        timedExecution("Data ingestion", () -> {
            for (int i = 0; i < THREADS; i++) {
                futures[i] = CompletableFuture.supplyAsync(() -> {
                    for (int c = counter.getAndIncrement(); c < NUM_ENTRIES; c = counter.getAndIncrement()) {
                        remoteCache.put(c, c);
                    }
                    return null;
                }, executorService);
            }
            CompletableFuture.allOf(futures).join();
        });

        timedExecution("Size", () -> {
            int size = remoteCache.size();
            System.out.printf("Ingested %d entries\n", size);
        });
    }

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration(builder));
    }

    private void warmup() {
        IntStream.range(0, 10).forEach(i -> iterate());
    }

    private void iterate() {
        AtomicInteger count = new AtomicInteger();
        CloseableIterator<Map.Entry<Object, Object>> iterator = remoteCache.retrieveEntries(null, 1000);
        iterator.forEachRemaining(o -> count.getAndIncrement());
        iterator.close();
    }

    @Test
    public void testIteration() {
        timedExecution("warmup", this::warmup);
        timedExecution("iteration", this::iterate);
        timedExecution("close cache manager", () -> remoteCacheManager.stop());
    }

    private static void timedExecution(String label, Runnable code) {
        long start = System.currentTimeMillis();
        code.run();
        System.out.format("Run %s in %d ms\n", label, System.currentTimeMillis() - start);
    }

}
