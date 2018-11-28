package org.infinispan.client.hotrod.tx;

import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.client.hotrod.transaction.manager.RemoteTransactionManager;
import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.test.ExceptionRunnable;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Tests Hot Rod client recovery
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.tx.RecoveryTest")
public class RecoveryTest extends MultiHotRodServersTest {

   private static final AtomicInteger XID_GENERATOR = new AtomicInteger(1);
   private final ControlledTimeService timeService = new ControlledTimeService(0);

   private static DummyXid newXid() {
      return new DummyXid((byte) XID_GENERATOR.getAndIncrement());
   }

   private static void assertNoTxException(ExceptionRunnable runnable) throws Exception {
      assertXaException(runnable, XAException.XAER_NOTA);
   }

   private static void assertInvalidException(ExceptionRunnable runnable) throws Exception {
      assertXaException(runnable, XAException.XAER_INVAL);
   }

   private static void assertXaException(ExceptionRunnable runnable, int errorCode) throws Exception {
      try {
         runnable.run();
         fail();
      } catch (XAException e) {
         assertEquals(errorCode, e.errorCode);
      }
   }

   public void testXaResourceReUse() throws Exception {
      XAResource xaResource = xaResource(0);

      DummyXid xid = newXid();
      assertNoTxException(() -> xaResource.start(xid, XAResource.TMJOIN));
      assertNoTxException(() -> xaResource.start(xid, XAResource.TMRESUME));
      assertNoTxException(() -> xaResource.end(xid, XAResource.TMNOFLAGS));
      assertNoTxException(() -> xaResource.prepare(xid));
      assertNoTxException(() -> xaResource.commit(xid, false));
      assertNoTxException(() -> xaResource.rollback(xid));

      Xid[] actual = xaResource.recover(XAResource.TMSTARTRSCAN);
      assertEquals(0, actual.length);
      actual = xaResource.recover(XAResource.TMNOFLAGS);
      assertEquals(0, actual.length);
      actual = xaResource.recover(XAResource.TMENDRSCAN);
      assertEquals(0, actual.length);

      //no-op
      xaResource.forget(xid);
   }

   public void testStartAndFinishScan() throws Exception {
      XAResource xaResource = xaResource(0);

      assertInvalidException(() -> xaResource.recover(XAResource.TMENDRSCAN));

      //2 start in a row should fail
      xaResource.recover(XAResource.TMSTARTRSCAN);
      assertInvalidException(() -> xaResource.recover(XAResource.TMSTARTRSCAN));
      xaResource.recover(XAResource.TMENDRSCAN);

      //start and end together is fine!
      xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);

      //no iteration in progress
      assertInvalidException(() -> xaResource.recover(XAResource.TMNOFLAGS));
   }

   public void testRecoveryIteration() throws Exception {
      XAResource xaResource0 = xaResource(0);
      XAResource xaResource1 = xaResource(1);

      //2 prepared transactions
      remoteTM(0).begin();
      remoteCache(0).put("k0", "v");
      Xid xid0 = xid(0);
      prepare(0);

      remoteTM(1).begin();
      remoteCache(1).put("k1", "v");
      Xid xid1 = xid(1);
      prepare(1);

      timeService.advance(9000); //9 seconds, below the 10 second configured

      assertBeforeTimeoutRecoveryIteration(xaResource0, xid0);
      assertBeforeTimeoutRecoveryIteration(xaResource1, xid1);

      timeService.advance(2000); //2 seconds, remote transaction will be include in recovery

      assertRecoveryIteration(xaResource0, xid0, xid1);
      assertRecoveryIteration(xaResource1, xid1, xid0);

      //resource1 if finished and it should be able to commit the xid0 transaction
      xaResource1.commit(xid0, false);
      xaResource1.rollback(xid1);

      assertEquals("v", remoteCache(0).get("k0"));
      assertEquals(null, remoteCache(0).get("k1"));

      xaResource0.forget(xid0);
      xaResource1.forget(xid1);
   }

   protected String cacheName() {
      return "recovery-test-cache";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cacheBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      cacheBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      createHotRodServers(numberOfNodes(), new ConfigurationBuilder());
      for (EmbeddedCacheManager cm : cacheManagers) {
         //use the same time service in all managers
         replaceComponent(cm, TimeService.class, timeService, true);
         //stop reaper. we are going to trigger it manually
         extractGlobalComponent(cm, GlobalTxTable.class).stop();
      }
      defineInAll(cacheName(), cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(
         int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = super
            .createHotRodClientConfigurationBuilder(serverPort);
      clientBuilder.forceReturnValues(false);
      clientBuilder.transaction().transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());
      clientBuilder.transaction().transactionMode(TransactionMode.FULL_XA);
      clientBuilder.transaction().timeout(10, TimeUnit.SECONDS);
      return clientBuilder;
   }

   private void assertRecoveryIteration(XAResource xaResource, Xid local, Xid remote) throws Exception {
      Xid[] actual = xaResource.recover(XAResource.TMSTARTRSCAN);
      assertTrue(actual.length != 0);
      if (actual.length == 1) {
         //it returned only the local transaction
         assertEquals(local, actual[0]);
         actual = xaResource.recover(XAResource.TMENDRSCAN);
         assertEquals(1, actual.length); //other client transaction
         assertEquals(remote, actual[0]);
      } else {
         //the server replied quick enough
         assertEquals(local, actual[0]);
         assertEquals(remote, actual[1]);
         actual = xaResource.recover(XAResource.TMENDRSCAN);
         assertEquals(0, actual.length);
      }
   }

   private void assertBeforeTimeoutRecoveryIteration(XAResource xaResource, Xid local) throws Exception {
      Xid[] actual = xaResource.recover(XAResource.TMSTARTRSCAN);
      assertEquals(1, actual.length);
      assertEquals(local, actual[0]);
      actual = xaResource.recover(XAResource.TMENDRSCAN);
      assertEquals(0, actual.length);
   }

   private void prepare(int index) throws Exception {
      RemoteTransactionManager tm = remoteTM(index);
      TransactionImpl tx = (TransactionImpl) tm.getTransaction();
      tm.suspend();
      assertTrue(tx.runPrepare());
   }

   private Xid xid(int index) {
      TransactionImpl tx = (TransactionImpl) remoteTM(index).getTransaction();
      return tx.getXid();
   }

   private XAResource xaResource(int index) throws Exception {
      RemoteTransactionManager tm = remoteTM(index);
      tm.begin();
      RemoteCache<String, String> cache = remoteCache(index);
      cache.put("_k_", "_v_");
      TransactionImpl tx = (TransactionImpl) tm.getTransaction();
      XAResource xaResource = tx.getEnlistedResources().iterator().next();
      tm.commit();
      xaResource.forget(tx.getXid());
      return xaResource;
   }

   private <K, V> RemoteCache<K, V> remoteCache(int index) {
      return client(index).getCache(cacheName());
   }

   private RemoteTransactionManager remoteTM(int index) {
      return (RemoteTransactionManager) remoteCache(index).getTransactionManager();
   }

   private int numberOfNodes() {
      return 3;
   }

   private static class DummyXid extends XidImpl {

      DummyXid(byte id) {
         super(-1234, new byte[]{id});
      }
   }

}
