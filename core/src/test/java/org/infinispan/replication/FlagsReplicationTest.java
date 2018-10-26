package org.infinispan.replication;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

/**
 * Verifies the Flags affect both local and remote nodes.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @since 4.2.1
 */
@Test(groups = "functional", testName = FlagsReplicationTest.TEST_NAME)
public class FlagsReplicationTest extends BaseDistFunctionalTest<Object, String> {

   static final String TEST_NAME = "replication.FlagsReplicationTest";
   static final String DATA_PROVIDER = TEST_NAME + ".dataprovider";

   private final Integer one = 1;
   private final String key = TEST_NAME;

   public FlagsReplicationTest() {
      transactional = true;
      cacheName = TEST_NAME;
      cleanup = CleanupPhase.AFTER_METHOD;
      lockingMode = LockingMode.PESSIMISTIC;
      lockTimeout = 1;
   }

   @DataProvider(name = DATA_PROVIDER)
   public Object[][] createTestConfigurations() {
      return new Object[][] {
               { true,  true  },
               { false, false },
               { false, true  },
               { true,  false },
         };
   }

   @Test(dataProvider = DATA_PROVIDER)
   public void testScenario(boolean cache1IsOwner, boolean cache2IsOwner) throws Throwable {
      log.tracef("Start cache1IsOwner = %s, cache2IsOwner %s", cache1IsOwner, cache2IsOwner);
      AdvancedCache cache1 = (cache1IsOwner ? getFirstOwner(key) : getFirstNonOwner(key)).getAdvancedCache();
      AdvancedCache cache2 = (cache2IsOwner ? getFirstOwner(key) : getFirstNonOwner(key)).getAdvancedCache();

      assertNull(cache1.put(key, one));

      log.trace("About to try to acquire a lock.");
      cache2.getTransactionManager().begin();
      if (! cache2.lock(key)) {
         fail("Could not acquire lock");
      }
      Transaction tx2 = cache2.getTransactionManager().suspend();

      cache1.getTransactionManager().begin();
      boolean locked = cache1.withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT, Flag.FAIL_SILENTLY).lock(key);
      assertFalse(locked);
      Object removed = cache1.withFlags(Flag.SKIP_LOCKING).remove(key);
      assertEquals(one, removed);
      Transaction tx1 = cache1.getTransactionManager().suspend();

      cache2.getTransactionManager().resume(tx2);
      cache2.getTransactionManager().commit();

      cache1.getTransactionManager().resume(tx1);
      cache1.getTransactionManager().commit();
      assertNull(cache2.get(key));
      log.tracef("End cache1IsOwner = %s, cache2IsOwner %s", cache1IsOwner, cache2IsOwner);
   }
}
