package org.infinispan.configuration.module;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "unit", testName = "configuration.module.ModuleConfigurationTest")
public class ModuleConfigurationTest {

   public void testModuleConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addModule(MyModuleConfigurationBuilder.class).attribute("testValue");

      Configuration configuration = builder.build();
      assertEquals(configuration.module(MyModuleConfiguration.class).attribute(), "testValue");

      ConfigurationBuilder secondBuilder = new ConfigurationBuilder();
      secondBuilder.read(configuration, Combine.DEFAULT);
      Configuration secondConfiguration = secondBuilder.build();
      assertEquals(secondConfiguration.module(MyModuleConfiguration.class).attribute(), "testValue");
   }
}
