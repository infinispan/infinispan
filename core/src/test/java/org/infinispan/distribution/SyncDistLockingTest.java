package org.infinispan.distribution;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.replication.SyncReplLockingTest;
import org.testng.annotations.Test;

/**
 * Tests for lock API
 * <p/>
 * Introduce lock() API methods https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distribution.SyncDistLockingTest")
public class SyncDistLockingTest extends SyncReplLockingTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
}