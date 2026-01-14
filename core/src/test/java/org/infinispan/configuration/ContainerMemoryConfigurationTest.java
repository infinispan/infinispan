package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.lang.reflect.Method;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.ContainerMemoryConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * Test for different scenarios of container memory configuration.
 */
@Test(groups = "unit", testName = "configuration.ContainerMemoryConfigurationTest")
public class ContainerMemoryConfigurationTest extends AbstractInfinispanTest {

   @Test
   public void testMemoryConfigurationWithMaxSize(Method m) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.containerMemoryConfiguration(m.getName()).maxSize("1.5 GB");

      GlobalConfiguration global = builder.build();

      ContainerMemoryConfiguration configuration = global.getMemoryContainer().get(m.getName());

      assertEquals(1_500_000_000, configuration.maxSizeBytes());
      assertEquals(-1, configuration.maxCount());
   }

   @Test
   public void testMemoryConfigurationWithMaxCount(Method m) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.containerMemoryConfiguration(m.getName()).maxCount(2000L);

      GlobalConfiguration global = builder.build();

      ContainerMemoryConfiguration configuration = global.getMemoryContainer().get(m.getName());

      assertEquals(-1, configuration.maxSizeBytes());
      assertEquals(2000, configuration.maxCount());
   }

   public void testUseDefaultMemoryConfiguration(Method m) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.containerMemoryConfiguration(m.getName());

      GlobalConfiguration global = builder.build();

      ContainerMemoryConfiguration configuration = global.getMemoryContainer().get(m.getName());

      assertFalse(configuration.isEvictionEnabled());
      assertEquals(-1, configuration.maxSizeBytes());
      assertEquals(-1, configuration.maxCount());
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = ".*Cannot configure both maxCount and maxSize in memory configuration.*")
   public void testMemoryConfigurationWithBothSizeAndCount(Method m) {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.containerMemoryConfiguration(m.getName()).maxSize("1.5 GB").maxCount(2000L);

      builder.build();
   }

   @Test
   public void testChangeFromDefault(Method m) {
      GlobalConfigurationBuilder initial = new GlobalConfigurationBuilder();
      initial.containerMemoryConfiguration(m.getName());
      ContainerMemoryConfiguration initialConfig = initial.build().getMemoryContainer().get(m.getName());
      assertEquals(-1, initialConfig.maxCount());

      initial.containerMemoryConfiguration(m.getName()).maxCount(3L);
      ContainerMemoryConfiguration larger = initial.build().getMemoryContainer().get(m.getName());
      assertEquals(3, larger.maxCount());
   }
}
