package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.topology.CacheShutdownCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@CleanupAfterMethod
@Test(groups = "functional", testName = "globalstate.TransactionalGracefulShutdownTest")
public class TransactionalGracefulShutdownTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";
   private static final int NUM_NODES = 2;

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < NUM_NODES; i++) {
         addCacheManager(i);
      }
   }

   private void addCacheManager(int index) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Character.toString('A' + index));

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);
      global.serialization().addContextInitializer(TestDataSCI.INSTANCE);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC);
      cb.persistence().addSoftIndexFileStore();
      EmbeddedCacheManager ecm = addClusterEnabledCacheManager(global, null);
      ecm.defineConfiguration(CACHE_NAME, cb.build());
   }

   public void testShutdownRollbackOperations() throws Throwable {
      Cache<MagicKey, String> c0 = testCache(0);
      Cache<MagicKey, String> c1 = testCache(1);

      MagicKey key = new MagicKey("remote-key", c1);

      assertThat(c1.put(key, "value")).isNull();
      assertThat(c0.get(key)).isEqualTo("value");

      CheckPoint commitCheckpoint = new CheckPoint();
      commitCheckpoint.triggerForever(Mocks.AFTER_RELEASE);

      CheckPoint shutdownCheckpoint = new CheckPoint();

      // One checkpoint will block after the commit operation is submitted and the other during the graceful shutdown.
      Mocks.blockInboundCacheRpcCommand(c1, commitCheckpoint, crc -> crc instanceof PrepareCommand);
      Mocks.blockInboundGlobalCommand(manager(1), shutdownCheckpoint, rc -> rc instanceof CacheShutdownCommand);

      Future<Void> tx = fork(() -> {
         TransactionManager tm = TestingUtil.getTransactionManager(c0);
         tm.begin();
         c0.put(key, "updated");
         tm.commit();
      });

      // After C1 receives the prepare command, it initiates the shutdown command BEFORE handling the command.
      commitCheckpoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);
      Future<?> shutdown = fork(c1::shutdown);
      shutdownCheckpoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Shutdown happens cluster-wide, and the TX table has a "degradation" period before giving up.
      assertThat(shutdown.isDone()).isFalse();

      // Release the shutdown of C1, it will stop all running components and reply CacheNotFound for commit command.
      shutdownCheckpoint.trigger(Mocks.BEFORE_RELEASE, 1);
      shutdownCheckpoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);
      commitCheckpoint.trigger(Mocks.BEFORE_RELEASE, 1);

      // Even though the shutdown future hasn't completed because it is cluster-wide, C1 is already shutdown.
      assertThat(ComponentRegistry.of(c1).getStatus().isTerminated()).isTrue();
      shutdownCheckpoint.triggerForever(Mocks.AFTER_RELEASE);

      // After some time, the shutdown will finally complete.
      // C0 will wait a period before giving up transactions, this makes the shutdown procedure to take a while.
      shutdown.get(30, TimeUnit.SECONDS);
      assertThat(tx.isDone()).isTrue();

      // We restart the cluster again. Since persistence is enabled, the data should be present and still the same.
      recreateCluster();

      assertThat(testCache(0).get(key)).isEqualTo("value");
      assertThat(testCache(1).get(key)).isEqualTo("value");
   }

   private void recreateCluster() {
      for (EmbeddedCacheManager ecm : cacheManagers) {
         ecm.stop();
      }

      cacheManagers.clear();
      createCacheManagers();
      waitForClusterToForm(CACHE_NAME);
   }

   private Cache<MagicKey, String> testCache(int index) {
      return cache(index, CACHE_NAME);
   }
}
