package org.infinispan.loaders.cassandra.configuration;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.cassandra.CassandraCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.cassandra.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testCassandraCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(CassandraCacheStoreConfigurationBuilder.class)
         .autoCreateKeyspace(false)
         .framed(true)
         .readConsistencyLevel(ConsistencyLevel.EACH_QUORUM)
         .writeConsistencyLevel(ConsistencyLevel.ANY)
         .fetchPersistentState(true)
         .addServer()
            .host("one")
         .addServer()
            .host("two")
         .async().enable();
      Configuration configuration = b.build();
      CassandraCacheStoreConfiguration store = (CassandraCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert !store.autoCreateKeyspace();
      assert store.framed();
      assert store.servers().size() == 2;
      assert store.readConsistencyLevel().equals(ConsistencyLevel.EACH_QUORUM);
      assert store.writeConsistencyLevel().equals(ConsistencyLevel.ANY);
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(CassandraCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      CassandraCacheStoreConfiguration store2 = (CassandraCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert !store2.autoCreateKeyspace();
      assert store2.framed();
      assert store2.servers().size() == 2;
      assert store2.readConsistencyLevel().equals(ConsistencyLevel.EACH_QUORUM);
      assert store2.writeConsistencyLevel().equals(ConsistencyLevel.ANY);
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      CassandraCacheStoreConfig legacy = store.adapt();
      assert !legacy.isAutoCreateKeyspace();
      assert legacy.isFramed();
      assert legacy.getReadConsistencyLevel().equals(ConsistencyLevel.EACH_QUORUM.toString());
      assert legacy.getWriteConsistencyLevel().equals(ConsistencyLevel.ANY.toString());
      assert legacy.isFetchPersistentState();
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}
