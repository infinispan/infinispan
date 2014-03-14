package org.infinispan.test.integration.as.cdi;

import static org.junit.Assert.assertEquals;

import javax.inject.Inject;

import org.infinispan.Version;
import org.infinispan.eviction.EvictionStrategy;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.spec.se.manifest.ManifestDescriptor;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 */
@RunWith(Arquillian.class)
public class GreetingCacheManagerIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "cdi-cm.war")
            .addPackage(GreetingCacheManagerIT.class.getPackage())
            .add(manifest(), "META-INF/MANIFEST.MF")
            .addAsWebInfResource("beans.xml");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan.cdi:" + Version.MODULE_SLOT + " services, org.infinispan.jcache:" + Version.MODULE_SLOT +" services").exportAsString();
      return new StringAsset(manifest);
   }

   @Inject
   private GreetingService greetingService;

   @Inject
   private GreetingCacheManager greetingCacheManager;

   @Before
   public void init() {
      greetingCacheManager.clearCache();
      assertEquals(0, greetingCacheManager.getNumberOfEntries());
   }

   @Test
   public void testGreetingCacheConfiguration() {
      // Cache name
      assertEquals("greeting-cache", greetingCacheManager.getCacheName());

      // Eviction
      assertEquals(128, greetingCacheManager.getEvictionMaxEntries());
      assertEquals(EvictionStrategy.LRU, greetingCacheManager.getEvictionStrategy());

      // Lifespan
      assertEquals(-1, greetingCacheManager.getExpirationLifespan());
   }

   @Test
   public void testGreetingCacheCachedValues() {
      greetingService.greet("Pete");

      assertEquals(1, greetingCacheManager.getCachedValues().length);
      assertEquals("Hello Pete :)", greetingCacheManager.getCachedValues()[0]);
   }

   @Test
   public void testClearGreetingCache() {
      greetingService.greet("Pete");

      assertEquals(1, greetingCacheManager.getNumberOfEntries());

      greetingCacheManager.clearCache();

      assertEquals(0, greetingCacheManager.getNumberOfEntries());
   }
}
