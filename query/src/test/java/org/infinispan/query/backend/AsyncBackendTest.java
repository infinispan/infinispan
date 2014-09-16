package org.infinispan.query.backend;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.indexmanager.IndexUpdateCommand;
import org.infinispan.query.test.Person;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;

/**
 * Tests for configuring a RemoteIndexingBackend so that it does not block on RPC
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.backend.AsyncBackendTest")
public class AsyncBackendTest extends AbstractInfinispanTest {

   @Test
   public void testWithEnablingAsync() {
      test(getBaseConfigPlus("default.worker.execution", "async"), new Assertion() {
         @Override
         public void doAssertion(RpcManager rpcManager) {
            calledIndexAsynchronously(rpcManager, "person");
         }
      });
   }

   @Test
   public void testWithEnablingAsyncForDifferentIndex() {
      test(getBaseConfigPlus("cat.worker.execution", "async"), new Assertion() {
         @Override
         public void doAssertion(RpcManager rpcManager) {
            calledIndexSynchronously(rpcManager, "person");
         }
      });
   }

   @Test
   public void testWithDefaultSettings() {
      test(getBaseConfig(), new Assertion() {
         @Override
         public void doAssertion(RpcManager rpcManager) {
            calledIndexSynchronously(rpcManager, "person");
         }
      });
   }

   @Test
   public void testWithShardedIndex() {
      ConfigurationBuilder cfg = getBaseConfigPlus(
            "default.sharding_strategy.nbr_of_shards", "2",
            "person.0.worker.execution", "async"
      );
      test(cfg, new Assertion() {
         @Override
         public void doAssertion(RpcManager rpcManager) {
            calledIndexAsynchronously(rpcManager, "person.0");
            calledIndexSynchronously(rpcManager, "person.1");
         }
      });
   }

   @Test
   public void testOverridingDefault() {
      ConfigurationBuilder cfg = getBaseConfigPlus(
            "default.worker.execution", "async",
            "person.worker.execution", "sync"
      );
      test(cfg, new Assertion() {
         @Override
         public void doAssertion(RpcManager rpcManager) {
            calledIndexSynchronously(rpcManager, "person");
         }
      });
   }

   @Test
   public void testHierarchy() {
      ConfigurationBuilder cfg = getBaseConfigPlus(
            "person.sharding_strategy.nbr_of_shards", "3",
            "default.worker.execution", "sync",
            "person.worker.execution", "async",
            "person.1.worker.execution", "sync"
      );
      test(cfg, new Assertion() {
         @Override
         public void doAssertion(RpcManager rpcManager) {
            calledIndexAsynchronously(rpcManager, "person.0");
            calledIndexSynchronously(rpcManager, "person.1");
            calledIndexAsynchronously(rpcManager, "person.2");
         }
      });
   }

   private ConfigurationBuilder getBaseConfig() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_SYNC).indexing().index(Index.ALL)
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager");
      return cfg;
   }

   private ConfigurationBuilder getBaseConfigPlus(String... props) {
      assert props != null && props.length % 2 == 0;
      ConfigurationBuilder cfg = getBaseConfig();
      for (int i = 0; i < props.length; i += 2) {
         cfg.indexing().addProperty(props[i], props[i + 1]);
      }
      return cfg;
   }

   @SuppressWarnings("unchecked")
   private void calledIndexSynchronously(RpcManager rpcManager, String indexName) {
      assertIndexCall(rpcManager, indexName, true);
   }

   @SuppressWarnings("unchecked")
   private void calledIndexAsynchronously(RpcManager rpcManager, String indexName) {
      assertIndexCall(rpcManager, indexName, false);
   }

   @SuppressWarnings("unchecked")
   private void assertIndexCall(RpcManager rpcManager, String indexName, boolean sync) {
      ArgumentCaptor<IndexUpdateCommand> argument = ArgumentCaptor.forClass(IndexUpdateCommand.class);
      RpcOptions rpcOptions = rpcManager.getDefaultRpcOptions(sync);
      verify(rpcManager, atLeastOnce()).invokeRemotely(anyCollection(), argument.capture(), eq(rpcOptions));
      boolean indexCalled = false;
      for (IndexUpdateCommand updateCommand : argument.getAllValues()) {
         indexCalled |= updateCommand.getParameters()[0].equals(indexName);
      }
      assertTrue(indexCalled);
   }

   private static interface Assertion {
      void doAssertion(RpcManager rpcManager);
   }

   private void test(ConfigurationBuilder cfg, final Assertion assertion) {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(cfg),
            TestCacheManagerFactory.createClusteredCacheManager(cfg)) {
         @Override
         public void call() {
            EmbeddedCacheManager slave = isMaster(cms[0].getCache()) ? cms[1] : cms[0];
            RpcManager wireTappedRpcManager = spyOnTransport(slave.getCache());
            slave.getCache().put(1, new Person("person1", "blurb1", 20));
            slave.getCache().put(2, new Person("person2", "blurb2", 27));
            slave.getCache().put(3, new Person("person3", "blurb3", 56));
            assertion.doAssertion(wireTappedRpcManager);
         }
      });
   }

   private boolean isMaster(Cache<?, ?> cm) {
      Transport transport = cm.getAdvancedCache().getRpcManager().getTransport();
      return transport.getCoordinator().equals(transport.getAddress());
   }

   private RpcManager spyOnTransport(Cache<?, ?> cache) {
      RpcManager rpcManager = spy(cache.getAdvancedCache().getRpcManager());
      replaceComponent(cache, RpcManager.class, rpcManager, false);
      return rpcManager;
   }

}
