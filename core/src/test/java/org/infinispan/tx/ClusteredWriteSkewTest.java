package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Test the write skew check in replicated ode
 *
 * Date: 1/28/12
 * Time: 10:29 AM
 *
 * @author pruivo
 */
@Test(groups = "functional", testName = "tx.ClusteredWriteSkewTest")
public class ClusteredWriteSkewTest extends MultipleCacheManagersTest {

    private static final int COUNTER_MAX_VALUE = 1000;
    private static final String COUNTER_KEY = "counter";

    @Override
    protected void createCacheManagers() throws Throwable {
        //Create the configuration: repeatable read with write skew, versioning, transactional and repl_sync
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
                .versioning().enabled(true).scheme(VersioningScheme.SIMPLE)
                .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
                .clustering().cacheMode(CacheMode.REPL_SYNC);
        createCluster(builder, 2);
        waitForClusterToForm();
    }

    public void testWriteSkew() {
        Cache<String, Integer> c1 = cache(0);
        Cache<String, Integer> c2 = cache(1);

        //initialize the counter
        c1.put(COUNTER_KEY, 0);

        //check if the counter is initialized in all caches
        assert c1.get(COUNTER_KEY) == 0 : "Initial value is different from zero in cache 1" ;
        assert c2.get(COUNTER_KEY) == 0 : "Initial value is different from zero in cache 2" ;

        //this will keep the values putted by both threads. any duplicated value will be detected because of the
        //return value of add() method
        ConcurrentSkipListSet<Integer> uniqueValuesIncremented = new ConcurrentSkipListSet<Integer>();

        //create both threads (simulate a node)
        IncrementCounterThread ict1 = new IncrementCounterThread("thread-node-1", c1, uniqueValuesIncremented);
        IncrementCounterThread ict2 = new IncrementCounterThread("thread-node-2", c2, uniqueValuesIncremented);

        try {
            //start and wait to finish
            ict1.start();
            ict2.start();

            ict1.join();
            ict2.join();
        } catch (InterruptedException e) {
            assert false : "Interrupted exception while running the test" ;
        }

        //check if all caches obtains the counter_max_values
        assert c1.get(COUNTER_KEY) >= COUNTER_MAX_VALUE : "Final value is less than " + COUNTER_MAX_VALUE +
                " in cache 1" ;
        assert c2.get(COUNTER_KEY) >= COUNTER_MAX_VALUE : "Final value is less than " + COUNTER_MAX_VALUE +
                " in cache 1" ;

        //check is any duplicated value is detected
        assert ict1.result : ict1.getName() + " has putted a duplicated value";
        assert ict2.result : ict2.getName() + " has putted a duplicated value";
    }

    private class IncrementCounterThread extends Thread {
        private Cache<String, Integer> cache;
        private ConcurrentSkipListSet<Integer> uniqueValuesSet;
        private TransactionManager transactionManager;
        private int lastValue;
        private boolean result = true;


        public IncrementCounterThread(String name, Cache<String, Integer> cache, ConcurrentSkipListSet<Integer> uniqueValuesSet) {
            super(name);
            this.cache = cache;
            this.transactionManager = cache.getAdvancedCache().getTransactionManager();
            this.uniqueValuesSet = uniqueValuesSet;
            this.lastValue = 0;
        }

        @Override
        public void run() {
            while (lastValue < COUNTER_MAX_VALUE) {
                try {
                    try {
                        //start transaction, get the counter value, increment and put it again
                        //check for duplicates in case of success
                        transactionManager.begin();

                        Integer value = cache.get(COUNTER_KEY);
                        value = value + 1;
                        lastValue = value;

                        cache.put(COUNTER_KEY, value);

                        transactionManager.commit();

                        result = uniqueValuesSet.add(value);
                    } catch (Throwable t) {
                        //lets rollback
                        transactionManager.rollback();
                    }
                } catch (Throwable t) {
                    //the only possible exception is thrown by the rollback. just ignore it
                } finally {
                    assert result : "Duplicate value found in " + getName() + " (value=" + lastValue + ")";
                }
            }
        }
    }

}