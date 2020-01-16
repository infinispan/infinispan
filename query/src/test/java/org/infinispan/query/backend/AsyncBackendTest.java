package org.infinispan.query.backend;

import static org.infinispan.test.TestingUtil.replaceField;
import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.indexmanager.IndexUpdateCommand;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.test.Person;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

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
      test(getBaseConfigPlus("default.worker.execution", "async"),
           transport -> calledIndexAsynchronously(transport, "person"));
   }

   @Test
   public void testWithEnablingAsyncForDifferentIndex() {
      test(getBaseConfigPlus("cat.worker.execution", "async"),
           transport -> calledIndexSynchronously(transport, "person"));
   }

   @Test
   public void testWithDefaultSettings() {
      test(getBaseConfig(), transport -> calledIndexSynchronously(transport, "person"));
   }

   @Test
   public void testWithShardedIndex() {
      ConfigurationBuilder cfg = getBaseConfigPlus(
            "default.sharding_strategy.nbr_of_shards", "2",
            "person.0.worker.execution", "async"
      );
      test(cfg, transport -> {
         calledIndexAsynchronously(transport, "person.0");
         calledIndexSynchronously(transport, "person.1");
      });
   }

   @Test
   public void testOverridingDefault() {
      ConfigurationBuilder cfg = getBaseConfigPlus(
            "default.worker.execution", "async",
            "person.worker.execution", "sync"
      );
      test(cfg, transport -> calledIndexSynchronously(transport, "person"));
   }

   @Test
   public void testHierarchy() {
      ConfigurationBuilder cfg = getBaseConfigPlus(
            "person.sharding_strategy.nbr_of_shards", "3",
            "default.worker.execution", "sync",
            "person.worker.execution", "async",
            "person.1.worker.execution", "sync"
      );
      test(cfg, transport -> {
         calledIndexAsynchronously(transport, "person.0");
         calledIndexSynchronously(transport, "person.1");
         calledIndexAsynchronously(transport, "person.2");
      });
   }

   private ConfigurationBuilder getBaseConfig() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(CacheMode.REPL_SYNC)
         .indexing().index(Index.ALL)
         .addIndexedEntity(Person.class)
         .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
         .addProperty("lucene_version", "LUCENE_CURRENT");
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

   private void calledIndexSynchronously(Transport transport, String indexName) throws Exception {
      assertIndexCall(transport, indexName, true);
   }

   private void calledIndexAsynchronously(Transport transport, String indexName) throws Exception {
      assertIndexCall(transport, indexName, false);
   }

   private void assertIndexCall(Transport transport, String indexName, boolean sync) throws Exception {
      ArgumentCaptor<IndexUpdateCommand> argument = ArgumentCaptor.forClass(IndexUpdateCommand.class);
      verify(transport, atLeastOnce()).invokeRemotelyAsync(anyCollection(), argument.capture(),
                                                           eq(sync ? ResponseMode.SYNCHRONOUS : ResponseMode.ASYNCHRONOUS),
                                                           anyLong(), any(), any(), anyBoolean());
      boolean indexCalled = false;
      for (IndexUpdateCommand updateCommand : argument.getAllValues()) {
         indexCalled |= updateCommand.getIndexName().equals(indexName);
      }
      assertTrue(indexCalled);
   }

   private interface Assertion {
      void doAssertion(Transport transport) throws Exception;
   }

   private void test(ConfigurationBuilder cfg, Assertion assertion) {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createClusteredCacheManager(cfg),
            TestCacheManagerFactory.createClusteredCacheManager(cfg)) {
         @Override
         public void call() throws Exception {
            EmbeddedCacheManager slave = isMaster(cms[0].getCache()) ? cms[1] : cms[0];
            Transport wireTappedTransport = spyOnTransport(slave.getCache());
            slave.getCache().put(1, new Person("person1", "blurb1", 20));
            slave.getCache().put(2, new Person("person2", "blurb2", 27));
            slave.getCache().put(3, new Person("person3", "blurb3", 56));
            assertion.doAssertion(wireTappedTransport);
         }
      });
   }

   private boolean isMaster(Cache<?, ?> cm) {
      Transport transport = cm.getAdvancedCache().getRpcManager().getTransport();
      return transport.getCoordinator().equals(transport.getAddress());
   }

   private Transport spyOnTransport(Cache<?, ?> cache) {
      RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      Transport transport = spy(rpcManager.getTransport());
      replaceField(transport, "t", rpcManager, RpcManagerImpl.class);
      return transport;
   }
}
