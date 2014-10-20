package org.infinispan.spring.support.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Bean that creates cache manager instances produced
 * by the test cache manager factory.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class TestInfinispanEmbeddedCacheManagerFactoryBean extends InfinispanEmbeddedCacheManagerFactoryBean {

   @Override
   protected EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      return TestCacheManagerFactory.createClusteredCacheManager(globalBuilder, builder);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(InputStream is) throws IOException {
      return TestCacheManagerFactory.fromStream(is);
   }

}
