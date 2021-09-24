package org.infinispan.persistence;

import java.util.stream.Stream;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.expiration.impl.CustomLoaderNonNullWithExpirationTest;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.PassivationOptionsTest")
public class PassivationOptionsTest extends AbstractInfinispanTest {
   public void testPassivationWithLoader() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence()
            .passivation(true)
            .addStore(CustomLoaderNonNullWithExpirationTest.SimpleLoaderConfigurationBuilder.class)
            .segmented(false);

      TestCacheManagerFactory.createCacheManager(builder);
   }

   @DataProvider(name = "passivationEnabled")
   public Object[][] maxIdlePassivationParams() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(passivationEnabled ->
                  Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(maxIdleEnabled -> new Object[]{passivationEnabled, maxIdleEnabled}))
            .toArray(Object[][]::new);
   }


   @Test(dataProvider = "passivationEnabled")
   public void testPassivationWithMaxIdle(boolean passivationEnabled, boolean maxIdleEnabled) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (maxIdleEnabled) {
         builder.expiration()
               .maxIdle(10);
      }
      builder.persistence()
            .passivation(passivationEnabled)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      try {
         TestCacheManagerFactory.createCacheManager(builder);
      } catch (Throwable t) {
         if (!passivationEnabled && maxIdleEnabled) {
            Exceptions.assertException(EmbeddedCacheManagerStartupException.class, CacheConfigurationException.class, ".*Max idle is not allowed.*", t);
         } else {
            throw t;
         }
      }
   }
}
