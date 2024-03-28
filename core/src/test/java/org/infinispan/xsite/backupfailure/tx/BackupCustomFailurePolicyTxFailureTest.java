package org.infinispan.xsite.backupfailure.tx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CustomFailurePolicy;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.xsite.BackupFailureException;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.testng.annotations.Test;


@Test(groups = "xsite", testName = "xsite.backupfailure.tx.BackupCustomFailurePolicyTxFailureTest")
public class BackupCustomFailurePolicyTxFailureTest extends BackupTxFailureTest {

   @Override
   protected void decorate(BackupConfigurationBuilder builder) {
      super.decorate(builder);
      builder
            .backupFailurePolicy(BackupFailurePolicy.CUSTOM)
            .failurePolicyClass("org.infinispan.xsite.backupfailure.tx.BackupCustomFailurePolicyTxFailureTest$CountingFailurePolicy");
   }

   @Override
   protected void assertAfterTest(Cache<String, String> cache) {
      super.assertAfterTest(cache);
      var sender = TestingUtil.extractComponent(cache, BackupSender.class);
      assertTrue(sender instanceof BackupSenderImpl);
      var policy = ((BackupSenderImpl) sender).getCustomFailurePolicy(siteName(1));
      assertNotNull(policy);
      assertTrue(policy instanceof CountingFailurePolicy);
      if (isTwoPhaseCommit(cache) || isPessimisticLocking(cache)) {
         // if 2PC cross-site or pessimistic locking, the failure will happen in prepare
         assertEquals(1, ((CountingFailurePolicy<Object, Object>) policy).prepareFailCount.getAndSet(0));
         assertEquals(0, ((CountingFailurePolicy<Object, Object>) policy).commitFailCount.getAndSet(0));
      } else {
         // otherwise, the failure will happen in commit
         assertEquals(0, ((CountingFailurePolicy<Object, Object>) policy).prepareFailCount.getAndSet(0));
         assertEquals(1, ((CountingFailurePolicy<Object, Object>) policy).commitFailCount.getAndSet(0));
      }

   }

   private boolean isTwoPhaseCommit(Cache<?, ?> cache) {
      return cache.getCacheConfiguration()
            .sites()
            .syncBackupsStream()
            .filter(backupConfiguration -> backupConfiguration.site().equals(siteName(1)))
            .findFirst()
            .map(BackupConfiguration::isTwoPhaseCommit)
            .orElse(Boolean.FALSE);
   }

   private boolean isPessimisticLocking(Cache<?, ?> cache) {
      return cache.getCacheConfiguration()
            .transaction()
            .lockingMode() == LockingMode.PESSIMISTIC;
   }

   public static class CountingFailurePolicy<K, V> implements CustomFailurePolicy<K, V> {

      final AtomicInteger prepareFailCount = new AtomicInteger();
      final AtomicInteger commitFailCount = new AtomicInteger();

      @Override
      public void init(Cache<K, V> cache) {

      }

      @Override
      public void handlePutFailure(String site, K key, V value, boolean putIfAbsent) {

      }

      @Override
      public void handleRemoveFailure(String site, K key, V oldValue) {

      }

      @Override
      public void handleReplaceFailure(String site, K key, V oldValue, V newValue) {

      }

      @Override
      public void handleClearFailure(String site) {

      }

      @Override
      public void handlePutAllFailure(String site, Map<K, V> map) {

      }

      @Override
      public void handlePrepareFailure(String site, Transaction transaction) {
         prepareFailCount.incrementAndGet();
         throw new BackupFailureException();
      }

      @Override
      public void handleRollbackFailure(String site, Transaction transaction) {

      }

      @Override
      public void handleCommitFailure(String site, Transaction transaction) {
         commitFailCount.incrementAndGet();
         throw new BackupFailureException();
      }
   }
}
