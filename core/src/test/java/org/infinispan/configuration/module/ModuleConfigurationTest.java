package org.infinispan.configuration.module;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.junit.Assert;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.module.ModuleConfigurationTest")
public class ModuleConfigurationTest {

   public void testModuleConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addModule(MyModuleConfigurationBuilder.class).attribute("testValue");

      Configuration configuration = builder.build();
      Assert.assertEquals(configuration.module(MyModuleConfiguration.class).attribute(), "testValue");

      ConfigurationBuilder secondBuilder = new ConfigurationBuilder();
      secondBuilder.read(configuration);
      Configuration secondConfiguration = secondBuilder.build();
      Assert.assertEquals(secondConfiguration.module(MyModuleConfiguration.class).attribute(), "testValue");
   }
}
