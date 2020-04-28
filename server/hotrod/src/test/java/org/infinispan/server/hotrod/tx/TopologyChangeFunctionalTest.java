package org.infinispan.server.hotrod.tx;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import javax.transaction.xa.XAResource;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodVersion;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.RemoteTransaction;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.server.hotrod.tx.table.functions.CreateStateFunction;
import org.infinispan.server.hotrod.tx.table.functions.TxFunction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ByteString;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Functional test for transaction involved topology changes.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@Test(groups = "functional", testName = "server.hotrod.tx.TopologyChangeFunctionalTest")
public class TopologyChangeFunctionalTest extends HotRodMultiNodeTest {

   private org.infinispan.configuration.cache.TransactionMode transactionMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            //TODO add optimistic tests when supported!
            new TopologyChangeFunctionalTest()
                  .transactionMode(org.infinispan.configuration.cache.TransactionMode.NON_XA).lockingMode(
                  LockingMode.PESSIMISTIC),
            new TopologyChangeFunctionalTest()
                  .transactionMode(org.infinispan.configuration.cache.TransactionMode.NON_DURABLE_XA).lockingMode(
                  LockingMode.PESSIMISTIC),
            new TopologyChangeFunctionalTest()
                  .transactionMode(org.infinispan.configuration.cache.TransactionMode.FULL_XA).lockingMode(
                  LockingMode.PESSIMISTIC)
      };
   }

   public TopologyChangeFunctionalTest transactionMode(
         org.infinispan.configuration.cache.TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   public void testNonOriginatorLeft(Method method) {
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

      killNode(1);

      tx.commitAndAssert(XAResource.XA_OK);
      tx.forget();

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testNodeJoin(Method method) {
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

      addNewNode();

      tx.commitAndAssert(XAResource.XA_OK);
      tx.forget();

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   @Test(groups = "unstable", description = "ISPN-8432")
   public void testOriginatorLeft(Method method) {
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

      killNode(0);

      //index 0 is removed, index 0 is the old index 1
      tx.commitAndAssert(clients().get(0), XAResource.XA_OK);
      tx.forget(clients().get(0));

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testOriginatorLeftBeforePrepare(Method method) {
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

      killNode(0);

      //set the tx state to running
      GlobalTxTable transactionTable = extractGlobalComponent(manager(0), GlobalTxTable.class);
      CacheXid cacheXid = new CacheXid(ByteString.fromString(cacheName()), tx.getXid());
      TxState state = transactionTable.getState(cacheXid);
      transactionTable.remove(cacheXid);
      TxFunction function = new CreateStateFunction(state.getGlobalTransaction(), false, 60000);
      transactionTable.update(cacheXid, function, 60000);

      //index 0 is removed, index 0 is the old index 1
      tx.prepareAndAssert(clients().get(0), XAResource.XA_OK);
      tx.commitAndAssert(clients().get(0), XAResource.XA_OK);
      tx.forget(clients().get(0));

      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   @Override
   protected byte protocolVersion() {
      return HotRodVersion.HOTROD_27.getVersion();
   }

   @Override
   protected String cacheName() {
      return "topology-change-tx-cache";
   }

   @Override
   protected String parameters() {
      return "[" + lockingMode + "/" + transactionMode + "]";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
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
   protected int nodeCount() {
      return 3;
   }

   @BeforeMethod(alwaysRun = true)
   private void checkNumberOfNodes() {
      while (servers().size() > nodeCount()) {
         killNode(servers().size() - 1);
      }
      while (servers().size() < nodeCount()) {
         addNewNode();
      }
   }

   private void addNewNode() {
      int nextServerPort = findHighestPort().orElseGet(HotRodTestingUtil::serverPort);
      nextServerPort += 50;

      HotRodServer server = startClusteredServer(nextServerPort); //it waits for view
      servers().add(server);
      clients().add(createClient(server, cacheName()));
   }

   private void assertData(byte[] key, byte[] value) {
      for (HotRodClient client : clients()) {
         assertSuccess(client.get(key, 0), value);
      }
   }

   private void assertServerTransactionTableEmpty() {
      for (Cache cache : caches(cacheName())) {
         PerCacheTxTable perCacheTxTable = extractComponent(cache, PerCacheTxTable.class);
         assertTrue(perCacheTxTable.isEmpty());
      }
      for (EmbeddedCacheManager cm : managers()) {
         GlobalTxTable globalTxTable = extractGlobalComponent(cm, GlobalTxTable.class);
         assertTrue(globalTxTable.isEmpty());
      }
   }

   private void killNode(int index) {
      killClient(clients().remove(index));
      stopClusteredServer(servers().remove(index));
   }
}
