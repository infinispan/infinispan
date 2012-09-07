/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.rehash;

import org.apache.log4j.Logger;
import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.distribution.group.Group;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author esalter
 */
@Test(testName = "org.infinispan.RehashStressTest", enabled = true)
public class RehashStressTest extends AbstractInfinispanTest {

    @AfterMethod(alwaysRun = true)
    public void stopAllCacheManageres() {
        while (!cacheManagers.isEmpty()) {
            cacheManagers.poll().stop();
        }
    }

    private static Logger log = Logger.getLogger(RehashStressTest.class.getName());
   /*
    * This test simulates concurrent threads submitting distributed executor
    * tasks to ISPN, at the same time a rehash occurs.. You should see that
    * during high contention for the same lock, on occasion, a rehash will
    * result in stale locks.
    */
    private static final int KEY_RANGE = 10;
    private static final int TEST_THREADS = 40;
    private static final int TEST_LOOPS = 3000;
    private static final String CACHE_NAME = "testCache";
    
    public static final int MAX_INTERVAL_BETWEEN_TASK = 1000;
    
            
    LinkedList<EmbeddedCacheManager> cacheManagers = new LinkedList<EmbeddedCacheManager>();
    Random random = new Random();

    @BeforeClass(alwaysRun=true)
    public static void beforeTest() {
        System.setProperty("java.net.preferIPv4Stack", "true");        
    }
    
    public void testRehash() throws IOException, InterruptedException {
        EmbeddedCacheManager cacheManager = buildCacheManager();
        cacheManagers.addLast(cacheManager);
        cacheManager.getCache(CACHE_NAME);


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

        private static final long serialVersionUID = 1L;
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
        EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml("configs/rehash-test-cluster.xml");
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
                DistributedExecutorService ispnExecutor = new DefaultExecutorService(cacheManager.getCache(CACHE_NAME));

                Future<String> z = ispnExecutor.submit(new TransactionTask(), key);
                log.info("Task result=" + z.get());
            } catch (Exception ex) {
                log.warn("error during executing task " + key, ex);
            }
        }
    }

    static class TransactionTask
            implements DistributedCallable<TestKey, Integer, String>, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        private Cache<TestKey, Integer> cache;
        private TransactionManager tm;
        private TestKey key;

        @Override
        public void setEnvironment(Cache<TestKey, Integer> cache, Set<TestKey> inputKeys) {
            log.info("Setting env..." + cache.getAdvancedCache().getCacheManager().getAddress() + ", keys: " + inputKeys);
            this.cache = cache;
            this.key = (TestKey) inputKeys.iterator().next();
            this.tm = cache.getAdvancedCache().getTransactionManager();
        }

        @Override
        public String call() throws Exception {
            try {
                tm.begin();
                return performWork();
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
        }

        private String performWork() {
            log.info( "Locking " + key);
            cache.getAdvancedCache().lock(key);

            Integer value = cache.get(key);
            if( value == null ) {
                value = new Integer(0);
            }
            
            cache.put(key, ++value);            
            return value.toString();
        }
    }

    class RemoveNodeTask implements Runnable {

        @Override
        public void run() {
            try {
                int size = cacheManagers.size();
                int index = random.nextInt(size);
                EmbeddedCacheManager cacheManager = cacheManagers.remove(index); //This is not thread safe, but should be ok for this test since the main thread is the only writer to this list.


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
                cacheManager.getCache(CACHE_NAME);
                cacheManagers.addLast(cacheManager);

            } catch (Exception e) {
            log.warn("Error during node addition", e);
            }
        }
    }
}
