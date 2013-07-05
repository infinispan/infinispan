package org.infinispan.cdi.test.cachemanager.embedded;

import org.infinispan.Cache;
import org.infinispan.cdi.test.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the default embedded cache configuration can be overridden.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.embedded.DefaultConfigurationTest")
public class DefaultConfigurationTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultConfigurationTest.class)
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   private Cache<?, ?> cache;

   public void testDefaultConfiguration() {
      assertEquals(cache.getCacheConfiguration().eviction().maxEntries(), 16);
      assertEquals(cache.getName(), DEFAULT_CACHE_NAME);
   }

   /**
    * Overrides the default embedded cache configuration used for the initialization of the default embedded cache
    * manager.
    */
   public static class Config {
      @Produces
      public Configuration customDefaultConfiguration() {
         return new ConfigurationBuilder()
               .eviction().maxEntries(16)
               .build();
      }
   }
}
