package org.infinispan.distribution.rehash;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.group.Group;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.logging.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author esalter
 */
@Test(groups = "stress", testName = "distribution.rehash.RehashStressTest", timeOut = 15*60*1000)
public class RehashStressTest extends AbstractInfinispanTest {

    @AfterMethod
    public void stopAllCacheManageres() {
        while (!cacheManagers.isEmpty()) {
            cacheManagers.poll().stop();
        }
    }

    private static Logger log = Logger.getLogger(RehashStressTest.class.getName());
   /*
    * This test simulates concurrent threads submitting executor
    * tasks to ISPN, at the same time a rehash occurs.. You should see that
    * during high contention for the same lock, on occasion, a rehash will
    * result in stale locks.
    */
    private static final int KEY_RANGE = 10;
    private static final int TEST_THREADS = 40;
    private static final int TEST_LOOPS = 30000;
    public static final int MAX_INTERVAL_BETWEEN_TASK = 1000;
    LinkedList<EmbeddedCacheManager> cacheManagers = new LinkedList<EmbeddedCacheManager>();
    Random random = new Random();

    public void testRehash() throws IOException, InterruptedException {
        EmbeddedCacheManager cacheManager = buildCacheManager();
        cacheManagers.addLast(cacheManager);
        cacheManager.getCache("serviceGroup");


        new AddNodeTask().run();

        new AddNodeTask().run();

        new AddNodeTask().run();

        Thread.sleep(3000);
        log.info("Start testing");



        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(TEST_THREADS);
        executor.prestartAllCoreThreads();
        for (int i = 0; i < TEST_LOOPS; i++) {
            executor.submit(new SimulateTask());
        }


        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(3000);
                if (i != 1) {
                    new AddNodeTask().run();  //2
                } else {
                    new RemoveNodeTask().run();
                }
            } catch (RuntimeException e) {
                log.warn("Error during add/remove node", e);
            }
        }

        log.info("Rehash phase is completed...");
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

    }

    static class TestKey implements Serializable {

        int key;

        @Group
        public String getGroup() {
            return String.valueOf(key);
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public TestKey(int key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final TestKey other = (TestKey) obj;
            if (this.key != other.key) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 29 * hash + this.key;
            return hash;
        }

        @Override
        public String toString() {
            return "TestKey{" + "key=" + key + '}';
        }
    }

    private EmbeddedCacheManager buildCacheManager() throws IOException {
        EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml("erm-cluster.xml");
        return cacheManager;
    }

   /**
    * Simulates a client task. There is a random delay before submitting it to
    * prevent flooding of the executor service.
    */
    class SimulateTask implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(random.nextInt(MAX_INTERVAL_BETWEEN_TASK)); //random sleep a while before really submit the task to ISPN
            } catch (InterruptedException ex) {
            }
            TestKey key = new TestKey(random.nextInt(KEY_RANGE));
            try {

                log.info("Submitting a task " + key);
                EmbeddedCacheManager cacheManager = cacheManagers.get(random.nextInt(cacheManagers.size()));
                ClusterExecutor executor = cacheManager.executor();
                AtomicReference<String> value = new AtomicReference<>();
                CompletableFuture<Void> future = executor.submitConsumer(new TransactionTask("serviceGroup", key), (a, v, t) -> {
                    if (t != null) {
                        throw new CacheException(t);
                    }
                    value.set(v);
                });
                log.info("Task result=" + future.thenApply(ignore -> value.get()).get());
            } catch (Exception ex) {
                log.warn("error during executing task " + key, ex);
            }
        }
    }

    static class TransactionTask
            implements Function<EmbeddedCacheManager, String>, Serializable {

        private final String cacheName;
        private final TestKey key;

        TransactionTask(String cacheName, TestKey key) {
            this.cacheName = cacheName;
            this.key = key;
        }

        @Override
        public String apply(EmbeddedCacheManager embeddedCacheManager) {
            try {
                Cache<TestKey, ?> cache = embeddedCacheManager.getCache(cacheName);
                TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
                try {
                    tm.begin();
                    return performWork(cache);
                } catch (Exception e) {
                    log.warn("error during perform work " + key, e);
                    tm.setRollbackOnly();
                    throw e;
                } finally {

                    int status = -1;
                    try {
                        status = tm.getStatus();
                    } catch (Exception e) {
                    }

                    if (status == Status.STATUS_ACTIVE) {
                        tm.commit();
                    } else {
                        tm.rollback();
                    }
                }
            } catch (Exception e) {
                throw new CacheException(e);
            }
        }

        private String performWork(Cache<TestKey, ?> cache) {
            log.info( "Locking " + key);
            cache.getAdvancedCache().lock(key);

            return "locked " + key;
        }
    }

    class RemoveNodeTask implements Runnable {

        @Override
        public void run() {
            try {
                int size = cacheManagers.size();
                int index = random.nextInt(size);
                EmbeddedCacheManager cacheManager = cacheManagers.remove(index); //This is not thread safe, but should be ok for this test since the main thread is the only writrer to this list.


            log.info("Shutting down " + cacheManager.getAddress());
                cacheManager.stop();
            log.info("Shut down " + cacheManager.getAddress() + " complete");
            } catch (Exception e) {
            log.warn("Error during node removal", e);
            }
        }
    }

    class AddNodeTask implements Runnable {

        @Override
        public void run() {
            try {

            log.info("Starting a new cache manager");
                EmbeddedCacheManager cacheManager = buildCacheManager();
                cacheManager.getCache("serviceGroup");
                cacheManagers.addLast(cacheManager);

            } catch (Exception e) {
            log.warn("Error during node addition", e);
            }
        }
    }
}
