package org.infinispan.lock.singlelock.pessimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractInitiatorCrashTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "lock.singlelock.pessimistic.InitiatorCrashPessimisticTest")
@CleanupAfterMethod
public class InitiatorCrashPessimisticTest extends AbstractInitiatorCrashTest {

   public InitiatorCrashPessimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.PESSIMISTIC, false);
   }

 public void testInitiatorNodeCrashesBeforePrepare2() throws Exception {

      Object k0 = getKeyForCache(0);
      Object k1 = getKeyForCache(1);
      Object k2 = getKeyForCache(2);

      tm(1).begin();
      cache(1).put(k0, "v0");
      cache(1).put(k1, "v1");
      cache(1).put(k2, "v2");

      assertLocked(cache(0), k0);
      assertEventuallyNotLocked(cache(1), k0);
      assertEventuallyNotLocked(cache(2), k0);

      assertEventuallyNotLocked(cache(0), k1);
      assertLocked(cache(1), k1);
      assertEventuallyNotLocked(cache(2), k1);

      assertEventuallyNotLocked(cache(0), k2);
      assertEventuallyNotLocked(cache(1), k2);
      assertLocked(cache(2), k2);

      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 1, 0);
      assert checkTxCount(2, 0, 1);

      killMember(1);

      assert caches().size() == 2;

      assertNotLocked(k0);
      assertNotLocked(k1);
      assertNotLocked(k2);
      eventually(new AbstractInfinispanTest.Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }
}
