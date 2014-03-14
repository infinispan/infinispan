package org.infinispan.test.integration.as.cdi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.cache.annotation.CacheKey;
import javax.inject.Inject;

import org.infinispan.Version;
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
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 */
@RunWith(Arquillian.class)
public class GreetingServiceIT {

   @Deployment
   public static Archive<?> deployment() {
      return ShrinkWrap
            .create(WebArchive.class, "cdi.war")
            .addPackage(GreetingServiceIT.class.getPackage())
            .add(manifest(), "META-INF/MANIFEST.MF")
            .addAsWebInfResource("beans.xml");
   }

   private static Asset manifest() {
      String manifest = Descriptors.create(ManifestDescriptor.class)
            .attribute("Dependencies", "org.infinispan.cdi:" + Version.MODULE_SLOT + " services, org.infinispan.jcache:" + Version.MODULE_SLOT + " services").exportAsString();
      return new StringAsset(manifest);
   }

   @Inject
   @GreetingCache
   private org.infinispan.Cache<CacheKey, String> greetingCache;

   @Inject
   private GreetingService greetingService;

   @Before
   public void init() {
      greetingCache.clear();
      assertEquals(0, greetingCache.size());
   }

   @Test
   public void testGreetMethod() {
      assertEquals("Hello Pete :)", greetingService.greet("Pete"));
   }

   @Test
   public void testGreetMethodCache() {
      greetingService.greet("Pete");

      assertEquals(1, greetingCache.size());
      assertTrue(greetingCache.values().contains("Hello Pete :)"));

      greetingService.greet("Manik");

      assertEquals(2, greetingCache.size());
      assertTrue(greetingCache.values().contains("Hello Manik :)"));

      greetingService.greet("Pete");

      assertEquals(2, greetingCache.size());
   }
}
