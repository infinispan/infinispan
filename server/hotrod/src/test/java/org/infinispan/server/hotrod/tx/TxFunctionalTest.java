package org.infinispan.server.hotrod.tx;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertKeyDoesNotExist;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.Constants;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.RemoteTransaction;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * Functional and validation test for transaction.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@Test(groups = "functional", testName = "server.hotrod.tx.TxFunctionalTest")
public class TxFunctionalTest extends HotRodMultiNodeTest {

   private org.infinispan.configuration.cache.TransactionMode transactionMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            //TODO add optimistic tests!
            new TxFunctionalTest()
                  .transactionMode(org.infinispan.configuration.cache.TransactionMode.NON_XA).lockingMode(
                  LockingMode.PESSIMISTIC),
            new TxFunctionalTest()
                  .transactionMode(org.infinispan.configuration.cache.TransactionMode.NON_DURABLE_XA).lockingMode(
                  LockingMode.PESSIMISTIC),
            new TxFunctionalTest()
                  .transactionMode(org.infinispan.configuration.cache.TransactionMode.FULL_XA).lockingMode(
                  LockingMode.PESSIMISTIC)
      };
   }

   public TxFunctionalTest transactionMode(org.infinispan.configuration.cache.TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   public void testKeyNotRead(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testKeyNotReadWithConcurrentTransaction(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");
      HotRodClient otherClient = clients().get(1);
      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);

      otherClient.put(k1, 0, 0, v1_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testKeyNonExisting(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));

      tx.getAndAssert(k1, null);
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.getAndAssert(k2, null);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();

   }

   public void testKeyNonExistingWithConflict(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, null);
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.getAndAssert(k2, null);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);

      clients().get(1).put(k1, 0, 0, v1_1);

      tx.prepareAndAssert(XAException.XA_RBROLLBACK);
      tx.rollbackAndAssert(XAResource.XA_OK);

      assertData(k1, v1_1);
      assertDataDoesNotExist(k2);
      assertServerTransactionTableEmpty();
   }

   public void testKeyRead(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");
      final byte[] v2_1 = v(method, "v2_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.set(k1, v1_1);
      tx.getAndAssert(k1, v1_1);
      tx.getAndAssert(k2, v2);
      tx.set(k2, v2_1);
      tx.getAndAssert(k2, v2_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertData(k1, v1_1);
      assertData(k2, v2_1);
      assertServerTransactionTableEmpty();
   }

   public void testKeyReadWithConflict(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");
      final byte[] v2_1 = v(method, "v2_1");
      final byte[] v1_1_1 = v(method, "v1_1_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.set(k1, v1_1);
      tx.getAndAssert(k1, v1_1);
      tx.getAndAssert(k2, v2);
      tx.set(k2, v2_1);
      tx.getAndAssert(k2, v2_1);

      clients().get(1).put(k1, 0, 0, v1_1_1);

      tx.prepareAndAssert(XAException.XA_RBROLLBACK);
      tx.rollbackAndAssert(XAResource.XA_OK);

      assertData(k1, v1_1_1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testRemoveWithKeyNotRead(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertDataDoesNotExist(k1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testRemoveWithKeyNotReadWithConcurrentTransaction(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");
      final byte[] v2_1 = v(method, "v2_1");
      final byte[] v1_1_1 = v(method, "v1_1_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.set(k1, v1_1);
      tx.getAndAssert(k1, v1_1);
      tx.set(k2, v2_1);
      tx.getAndAssert(k2, v2_1);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      clients().get(1).put(k1, 0, 0, v1_1_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertDataDoesNotExist(k1);
      assertData(k2, v2_1);
      assertServerTransactionTableEmpty();
   }

   public void testRemoveWithNonExisting(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");


      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, null);
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.getAndAssert(k2, null);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertDataDoesNotExist(k1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testRemoveWithNonExistingWithConflictingTransaction(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");


      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, null);
      tx.set(k1, v1);
      tx.getAndAssert(k1, v1);
      tx.getAndAssert(k2, null);
      tx.set(k2, v2);
      tx.getAndAssert(k2, v2);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      clients().get(1).put(k1, 0, 0, v1_1);

      tx.prepareAndAssert(XAException.XA_RBROLLBACK);
      tx.rollbackAndAssert(XAResource.XA_OK);

      assertData(k1, v1_1);
      assertDataDoesNotExist(k2);
      assertServerTransactionTableEmpty();
   }

   public void testRemoveKeyRead(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v2_1 = v(method, "v2_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.remove(k1);
      tx.getAndAssert(k1, null);
      tx.getAndAssert(k2, v2);
      tx.set(k2, v2_1);
      tx.getAndAssert(k2, v2_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertDataDoesNotExist(k1);
      assertData(k2, v2_1);
      assertServerTransactionTableEmpty();
   }

   public void testRemoveKeyReadWithConflicting(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");
      final byte[] v2_1 = v(method, "v2_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.remove(k1);
      tx.getAndAssert(k1, null);
      tx.getAndAssert(k2, v2);
      tx.set(k2, v2_1);
      tx.getAndAssert(k2, v2_1);

      clients().get(1).put(k1, 0, 0, v1_1);

      tx.prepareAndAssert(XAException.XA_RBROLLBACK);
      tx.rollbackAndAssert(XAResource.XA_OK);

      assertData(k1, v1_1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testReadReadConflict(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");
      final byte[] v2_1 = v(method, "v2_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      clients().get(1).put(k2, 0, 0, v2_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertDataDoesNotExist(k1);
      assertData(k2, v2_1);
      assertServerTransactionTableEmpty();

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.set(k1, v1_1);
      tx.getAndAssert(k1, v1_1);

      clients().get(1).put(k2, 0, 0, v2_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(XAResource.XA_OK);

      assertData(k1, v1_1);
      assertData(k2, v2_1);
      assertServerTransactionTableEmpty();
   }

   public void testCommitFromAnotherNode(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(clients().get(1), XAResource.XA_OK);

      assertDataDoesNotExist(k1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.set(k1, v1_1);
      tx.getAndAssert(k1, v1_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.commitAndAssert(clients().get(1), XAResource.XA_OK);

      assertData(k1, v1_1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testRollbackFromAnotherNode(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");
      final byte[] v1_1 = v(method, "v1_1");

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.remove(k1);
      tx.getAndAssert(k1, null);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.rollbackAndAssert(clients().get(1), XAResource.XA_OK);

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();

      clients().get(1).put(k1, 0, 0, v1);
      clients().get(1).put(k2, 0, 0, v2);

      tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.getAndAssert(k1, v1);
      tx.set(k1, v1_1);
      tx.getAndAssert(k1, v1_1);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.rollbackAndAssert(clients().get(1), XAResource.XA_OK);

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testPrepareOnDifferentNode(Method method) {
      final byte[] k1 = k(method, "k1");
      final byte[] k2 = k(method, "k2");
      final byte[] v1 = v(method, "v1");
      final byte[] v2 = v(method, "v2");


      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().get(0));
      tx.set(k1, v1);
      tx.set(k2, v2);
      tx.getAndAssert(k1, v1);
      tx.getAndAssert(k2, v2);

      tx.prepareAndAssert(XAResource.XA_OK);
      tx.prepareAndAssert(clients().get(1), XAResource.XA_OK);

      tx.commitAndAssert(XAResource.XA_OK);

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();

   }

   @Override
   protected String parameters() {
      return "[" + lockingMode + "/" + transactionMode + "]";
   }

   @Override
   protected String cacheName() {
      return "tx-cache";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.transaction().lockingMode(lockingMode);
      switch (transactionMode) {
         case NON_XA:
            builder.transaction().useSynchronization(true);
            break;
         case NON_DURABLE_XA:
            builder.transaction().useSynchronization(false);
            builder.transaction().recovery().disable();
            break;
         case FULL_XA:
            builder.transaction().useSynchronization(false);
            builder.transaction().recovery().enable();
            break;
         default:
            throw new IllegalStateException();
      }
      builder.clustering().hash().numOwners(2);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      return builder;
   }

   @Override
   protected byte protocolVersion() {
      return Constants.VERSION_27;
   }

   private void assertDataDoesNotExist(byte[] key) {
      for (HotRodClient client : clients()) {
         assertKeyDoesNotExist(client.get(key, 0));
      }
   }

   private void assertData(byte[] key, byte[] value) {
      for (HotRodClient client : clients()) {
         assertSuccess(client.get(key, 0), value);
      }
   }

   private void assertServerTransactionTableEmpty() {
      for (Cache cache : caches(cacheName())) {
         ServerTransactionTable serverTransactionTable = TestingUtil
               .extractComponent(cache, ServerTransactionTable.class);
         assertTrue(serverTransactionTable.isEmpty());
      }
   }
}
