package org.infinispan.server.resp.test;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;

public interface TestSetup {

   int clusterSize();

   EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<ConfigurationBuilder> decorator);

   static TestSetup singleNodeTestSetup() {
      return SingleNodeTestSetup.INSTANCE;
   }

   static TestSetup clusteredTestSetup(int numNodes) {
      return new MultiNodeTestSetup(numNodes);
   }

   class SingleNodeTestSetup implements TestSetup {
      private static final TestSetup INSTANCE = new SingleNodeTestSetup();

      private SingleNodeTestSetup() { }

      @Override
      public int clusterSize() {
         return 1;
      }

      @Override
      public EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<ConfigurationBuilder> decorator) {
         GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
         TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
         ConfigurationBuilder builder = base.get();
         decorator.accept(builder);
         return TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, builder);
      }
   }

   record MultiNodeTestSetup(int clusterSize) implements TestSetup {

      @Override
         public EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<ConfigurationBuilder> decorator) {
            ConfigurationBuilder builder = base.get();
            decorator.accept(builder);
            return TestCacheManagerFactory.createClusteredCacheManager(builder);
         }
      }
}
