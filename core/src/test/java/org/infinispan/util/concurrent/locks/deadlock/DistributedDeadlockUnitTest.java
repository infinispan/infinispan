package org.infinispan.util.concurrent.locks.deadlock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.functional.FunctionalTestUtils.await;

import java.util.Collections;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.DeadlockDetection;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;

@Test(groups = "unit", testName = "deadlock.DistributedDeadlockUnitTest")
public class DistributedDeadlockUnitTest extends SingleCacheManagerTest {


   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cb.transaction().deadlockDetection(true);
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return TestCacheManagerFactory.createClusteredCacheManager(cb);
   }


   public void testTransactionRollback() throws Exception {
      Cache<String, String> c = cache();
      DistributedDeadlockDetection ddl = (DistributedDeadlockDetection) TestingUtil.extractComponent(c, DeadlockDetection.class);
      TransactionTable tt = TestingUtil.getTransactionTable(c);
      TransactionManager tm = TestingUtil.getTransactionManager(c);

      tm.begin();
      c.put("key", "value");
      GlobalTransaction gtx = tt.getGlobalTransaction(tm.getTransaction());
      CompletionStage<Void> cs = ddl.verifyDeadlockCycle(gtx, gtx, Collections.emptyList());
      await(cs);

      assertThat(tm.getStatus()).isEqualTo(Status.STATUS_MARKED_ROLLBACK);
      assertThatThrownBy(tm::commit).isInstanceOf(RollbackException.class);
   }
}
