package org.infinispan.server.hotrod.tx;

import static org.infinispan.server.hotrod.LifecycleCallbacks.GLOBAL_TX_TABLE_CACHE_NAME;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAResource;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.TransactionMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodVersion;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.RemoteTransaction;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.server.hotrod.test.TxResponse;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * It tests the transaction is completed in all the cluster members even if one of them is not able to apply the
 * completion command because it already installed a newer topology.
 * <p>
 * When the transaction originator leaves the cluster, the transaction is replayed by the node that receives the
 * client's completion request. A member that moved to a newer topology replies with {@link UnsureResponse} and doesn't
 * apply the command. If that reply is ignored, a commit silently loses the transaction's modifications in that member
 * and a rollback leaves the transaction (and its locks) behind.
 * <p>
 * The test runs twice: once with the originator as the primary owner of the keys and once with the originator not
 * owning them at all.
 *
 * @since 16.3
 */
@Test(groups = "functional", testName = "server.hotrod.tx.CompleteTransactionOutdatedTopologyTest")
public class CompleteTransactionOutdatedTopologyTest extends HotRodMultiNodeTest {

   private boolean originatorOwnsKeys = true;

   @Override
   public Object[] factory() {
      return new Object[]{
            new CompleteTransactionOutdatedTopologyTest().originatorOwnsKeys(true),
            new CompleteTransactionOutdatedTopologyTest().originatorOwnsKeys(false)
      };
   }

   public void testCommitWhenMemberHasNewerTopology(Method method) throws Exception {
      byte[] k1 = key();
      byte[] k2 = key();
      byte[] v1 = v(method, "v1");
      byte[] v2 = v(method, "v2");

      RemoteTransaction tx = RemoteTransaction.startTransaction(clients().getFirst());
      tx.set(k1, v1);
      tx.set(k2, v2);
      tx.prepareAndAssert(XAResource.XA_OK);

      //the originator is gone. the transaction has to be replayed by the node handling the commit request.
      killNode(0);

      HotRodClient client = clients().getFirst();
      assertEquals(XAResource.XA_OK, completeWithOutdatedMember(PrepareCommand.class, () -> client.commitTx(tx.getXid())));

      tx.forget(client);

      //the commit must be applied everywhere, including in the member that replied it had a newer topology.
      assertData(k1, v1);
      assertData(k2, v2);
      assertServerTransactionTableEmpty();
   }

   public void testRollbackWhenMemberHasNewerTopology(Method method) throws Exception {
      byte[] k1 = key();
      byte[] k2 = key();
      byte[] v1 = v(method, "v1");
      byte[] v2 = v(method, "v2");

      HotRodClient originator = clients().getFirst();
      originator.put(k1, 0, 0, v1);
      originator.put(k2, 0, 0, v2);

      RemoteTransaction tx = RemoteTransaction.startTransaction(originator);
      tx.set(k1, v(method, "v1-tx"));
      tx.set(k2, v(method, "v2-tx"));
      tx.prepareAndAssert(XAResource.XA_OK);

      //the originator is gone. the transaction has to be replayed by the node handling the rollback request.
      killNode(0);

      HotRodClient client = clients().getFirst();
      assertEquals(XAResource.XA_OK, completeWithOutdatedMember(RollbackCommand.class, () -> client.rollbackTx(tx.getXid())));

      tx.forget(client);

      //the rollback must reach every member, so none of them keeps the transaction's modifications.
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
      return "outdated-topology-tx-cache";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.transaction()
            .mode(TransactionMode.NON_XA)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      return builder;
   }

   @Override
   protected int nodeCount() {
      return 3;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "originatorOwnsKeys");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), originatorOwnsKeys);
   }

   public CompleteTransactionOutdatedTopologyTest originatorOwnsKeys(boolean originatorOwnsKeys) {
      this.originatorOwnsKeys = originatorOwnsKeys;
      return this;
   }

   /**
    * Completes the transaction while a member replies it already installed a newer topology.
    *
    * @return The XA code replied to the client.
    */
   private <C extends ReplicableCommand> int completeWithOutdatedMember(Class<C> commandClass,
         Callable<TestResponse> completeRequest) throws Exception {
      ControlledRpcManager rpcManager = ControlledRpcManager.replaceRpcManager(cache(0, cacheName()));
      try {
         Future<TestResponse> complete = fork(completeRequest);
         ControlledRpcManager.BlockedRequest<C> blockedCommand = rpcManager.expectCommand(commandClass);
         //the command is pinned to the topology below. from now on, the cluster can make progress without this test.
         rpcManager.stopBlocking();

         //a new member installs a newer topology, making the blocked command outdated.
         addNewNode();

         //the second member replies it isn't able to apply the command. the transaction must be replayed there.
         blockedCommand.skipSend().receive(address(1), UnsureResponse.INSTANCE);

         return ((TxResponse) complete.get(30, TimeUnit.SECONDS)).xaCode;
      } finally {
         rpcManager.revertRpcManager();
      }
   }

   /**
    * @return A key owned, or not, by the node that is about to leave the cluster.
    */
   private byte[] key() {
      Cache<?, ?> originator = cache(0, cacheName());
      //qualified: MultipleCacheManagersTest declares a getKeyForCache(Cache) returning a MagicKey.
      return originatorOwnsKeys ?
            HotRodTestingUtil.getKeyForCache(originator) :
            HotRodTestingUtil.getKeyNotOwnedBy(originator);
   }

   private void addNewNode() {
      int nextServerPort = ServerTestingUtil.findFreePort();
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
      for (Cache<?, ?> cache : caches(cacheName())) {
         assertTrue(TestingUtil.extractComponent(cache, PerCacheTxTable.class).isEmpty());
      }
      for (EmbeddedCacheManager cm : managers()) {
         assertTrue(TestingUtil.extractGlobalComponent(cm, GlobalTxTable.class).isEmpty());
      }
   }

   private void killNode(int index) {
      // kill==stop and it waits for the transactions to complete.
      // we drop all the transaction from the table to shutdown faster
      var txTable = TestingUtil.extractComponent(cache(index, cacheName()), TransactionTable.class);
      List.copyOf(txTable.getLocalTransactions()).forEach(txTable::removeLocalTransaction);
      List.copyOf(txTable.getRemoteGlobalTransaction()).forEach(txTable::removeRemoteTransaction);

      killClient(clients().remove(index));
      stopClusteredServer(servers().remove(index));
      TestingUtil.waitForNoRebalance(caches(cacheName()));
      TestingUtil.waitForNoRebalance(caches(GLOBAL_TX_TABLE_CACHE_NAME));
   }
}
