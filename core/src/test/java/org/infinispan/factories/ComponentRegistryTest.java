package org.infinispan.factories;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the concurrent lookup of components for ISPN-2796.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "factories.ComponentRegistryTest")
public class ComponentRegistryTest extends AbstractInfinispanTest {
   private GlobalComponentRegistry gcr;
   private ComponentRegistry cr1;
   private ComponentRegistry cr2;
   private TestDelayFactory.Control control;

   @BeforeMethod
   public void setUp() throws InterruptedException, ExecutionException {
      GlobalConfiguration gc = new GlobalConfigurationBuilder().build();
      Configuration c = new ConfigurationBuilder().build();
      Set<String> cachesSet = new HashSet<String>();
      EmbeddedCacheManager cm = mock(EmbeddedCacheManager.class);
      AdvancedCache cache = mock(AdvancedCache.class);

      gcr = new GlobalComponentRegistry(gc, cm, cachesSet);
      cr1 = new ComponentRegistry("cache", c, cache, gcr, ComponentRegistryTest.class.getClassLoader());
      cr2 = new ComponentRegistry("cache", c, cache, gcr, ComponentRegistryTest.class.getClassLoader());

      control = new TestDelayFactory.Control();
      gcr.registerComponent(control, TestDelayFactory.Control.class);
   }

   public void testSingleThreadLookup() throws InterruptedException, ExecutionException {
      control.unblock();

      TestDelayFactory.Component c1 = cr1.getOrCreateComponent(TestDelayFactory.Component.class);
      assertNotNull(c1);

      TestDelayFactory.Component c2 = cr1.getOrCreateComponent(TestDelayFactory.Component.class);
      assertNotNull(c2);
   }

   public void testConcurrentLookupSameComponentRegistry() throws Exception {
      testConcurrentLookup(cr1, cr2);
   }

   public void testConcurrentLookupDifferentComponentRegistries() throws Exception {
      testConcurrentLookup(cr1, cr2);
   }

   private void testConcurrentLookup(ComponentRegistry cr1, ComponentRegistry cr2) throws Exception {
      Future<TestDelayFactory.Component> future1 = fork(
         () -> cr1.getOrCreateComponent(TestDelayFactory.Component.class));
      Future<TestDelayFactory.Component> future2 = fork(
         () -> cr2.getOrCreateComponent(TestDelayFactory.Component.class));

      Thread.sleep(500);
      assertFalse(future1.isDone());
      assertFalse(future2.isDone());

      control.unblock();
      assertNotNull(future1.get());
      assertNotNull(future2.get());
   }

   public void testGetLocalComponent() {
      GlobalComponentRegistry localGcr = cr1.getLocalComponent(GlobalComponentRegistry.class);
      assertNull(localGcr);
   }
}
