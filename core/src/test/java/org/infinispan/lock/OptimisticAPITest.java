package org.infinispan.lock;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "lock.OptimisticAPITest")
public class OptimisticAPITest extends SingleCacheManagerTest {

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        final ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
        c.transaction().lockingMode(LockingMode.OPTIMISTIC).useSynchronization(true)
            .transactionManagerLookup(new DummyTransactionManagerLookup());
        return TestCacheManagerFactory.createCacheManager(c);
    }

    public void testFailSilentShouldNotBeIgnored() throws Exception {
        TransactionManager tm = tm();
        AdvancedCache<Integer, String> cache = this.<Integer, String>cache().getAdvancedCache()
                .withFlags(Flag.FAIL_SILENTLY, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);

        tm.begin();
        cache.put(1, "one");
        Transaction t1 = tm.suspend();

        tm.begin();
        cache.put(1, "uno");
        Transaction t2 = tm.suspend();

        tm.resume(t1);
        assertTrue(prepare());

        tm.resume(t2);
        assertTrue(prepare());

        tm.resume(t1);
        commit();

        tm.resume(t2);
        commit();
    }

    private boolean prepare() {
        DummyTransactionManager dtm = (DummyTransactionManager) tm();
        return dtm.getTransaction().runPrepare();
    }

    private void commit() {
        DummyTransactionManager dtm = (DummyTransactionManager) tm();
        try {
            dtm.getTransaction().runCommit(false);
        } catch (HeuristicMixedException | HeuristicRollbackException e) {
            throw new RuntimeException(e);
        }
    }

}
