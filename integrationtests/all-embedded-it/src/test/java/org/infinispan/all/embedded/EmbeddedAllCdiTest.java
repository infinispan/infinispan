package org.infinispan.all.embedded;

import static org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME;
import static org.junit.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.all.embedded.util.DefaultTestEmbeddedCacheManagerProducer;
import org.infinispan.all.embedded.util.EmbeddedUtils;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * 
 * Test for CDI included in infinispan-embedded uber-jar
 * 
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@RunWith(Arquillian.class)
public class EmbeddedAllCdiTest {
   
   @Deployment
   public static Archive<?> deployment() {
      return EmbeddedUtils.getCDIBaseDeployment()
            .addClass(EmbeddedAllCdiTest.class)
            .addClass(DefaultTestEmbeddedCacheManagerProducer.class);
   }

   @Inject
   private Cache<String, String> cache;

   @Inject
   private AdvancedCache<String, String> advancedCache;

   @Test
   public void testDefaultCache() {
      // Simple test to make sure the default cache works
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");
      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");
      assertEquals(cache.getName(), DEFAULT_CACHE_NAME);
      /*
       * Check that the advanced cache contains the same data as the simple
       * cache. As we can inject either Cache or AdvancedCache, this is double
       * checking that they both refer to the same underlying impl and Seam
       * Clouds isn't returning the wrong thing.
       */
      assertEquals(advancedCache.get("pete"), "British");
      assertEquals(advancedCache.get("manik"), "Sri Lankan");
      assertEquals(advancedCache.getName(), DEFAULT_CACHE_NAME);
   }

}
