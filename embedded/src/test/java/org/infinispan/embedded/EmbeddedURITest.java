package org.infinispan.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.junit.jupiter.api.Test;

public class EmbeddedURITest {
   @Test
   public void testInfinispanLocal() {
      EmbeddedURI uri = EmbeddedURI.create("infinispan:local://infinispan?cache-container.jndi-name=JNDI");
      GlobalConfiguration gc = uri.toConfiguration().getGlobalConfigurationBuilder().build();
      assertEquals("infinispan", gc.cacheManagerName());
      assertFalse(gc.isClustered());
   }

   @Test
   public void testInfinispanCluster() {
      EmbeddedURI uri = EmbeddedURI.create("infinispan:cluster://infinispan");
      GlobalConfiguration gc = uri.toConfiguration().getGlobalConfigurationBuilder().build();
      assertTrue(gc.isClustered());
   }

   @Test
   public void testInfinispanClasspath() {
      EmbeddedURI uri = EmbeddedURI.create("infinispan:classpath:///infinispan-uri.xml");
      GlobalConfiguration gc = uri.toConfiguration().getGlobalConfigurationBuilder().build();
      assertEquals(gc.cacheManagerName(), "uri");
   }

   @Test
   public void testClasspath() {
      EmbeddedURI uri = EmbeddedURI.create("classpath:///infinispan-uri.xml");
      GlobalConfiguration gc = uri.toConfiguration().getGlobalConfigurationBuilder().build();
      assertEquals(gc.cacheManagerName(), "uri");
   }

   @Test
   public void testInfinispanFile() {
      String property = System.getProperty("build.directory");
      EmbeddedURI uri = EmbeddedURI.create("infinispan:file://" + property + "/test-classes/infinispan-uri.xml");
      GlobalConfiguration gc = uri.toConfiguration().getGlobalConfigurationBuilder().build();
      assertEquals(gc.cacheManagerName(), "uri");
   }

   @Test
   public void testFile() {
      String property = System.getProperty("build.directory");
      EmbeddedURI uri = EmbeddedURI.create("file://" + property + "/test-classes/infinispan-uri.xml");
      GlobalConfiguration gc = uri.toConfiguration().getGlobalConfigurationBuilder().build();
      assertEquals(gc.cacheManagerName(), "uri");
   }
}
