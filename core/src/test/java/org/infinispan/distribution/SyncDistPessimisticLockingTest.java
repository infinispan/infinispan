package org.infinispan.distribution;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.replication.SyncReplPessimisticLockingTest;

import org.testng.annotations.Test;

/**
 * Tests for implicit locking
 * <p/>
 * Transparent eager locking for transactions https://jira.jboss.org/jira/browse/ISPN-70
 *
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "distribution.SyncDistPessimisticLockingTest")
public class SyncDistPessimisticLockingTest extends SyncReplPessimisticLockingTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
 }
