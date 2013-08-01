package org.infinispan.loaders.remote.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.remote.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testRemoteCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class)
         .remoteCacheName("RemoteCache")
         .fetchPersistentState(true)
         .addServer()
            .host("one").port(12111)
         .addServer()
            .host("two")
         .connectionPool()
            .maxActive(10)
            .minIdle(5)
            .exhaustedAction(ExhaustedAction.EXCEPTION)
            .minEvictableIdleTime(10000)
         .async().enable();
      Configuration configuration = b.build();
      RemoteCacheStoreConfiguration store = (RemoteCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.remoteCacheName().equals("RemoteCache");
      assert store.servers().size() == 2;
      assert store.connectionPool().maxActive() == 10;
      assert store.connectionPool().minIdle() == 5;
      assert store.connectionPool().exhaustedAction() == ExhaustedAction.EXCEPTION;
      assert store.connectionPool().minEvictableIdleTime() == 10000;
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(RemoteCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      RemoteCacheStoreConfiguration store2 = (RemoteCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.remoteCacheName().equals("RemoteCache");
      assert store2.servers().size() == 2;
      assert store2.connectionPool().maxActive() == 10;
      assert store2.connectionPool().minIdle() == 5;
      assert store2.connectionPool().exhaustedAction() == ExhaustedAction.EXCEPTION;
      assert store2.connectionPool().minEvictableIdleTime() == 10000;
      assert store2.fetchPersistentState();
      assert store2.async().enabled();
   }
}