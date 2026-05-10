package org.infinispan.configuration.module;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "configuration.module.ModuleConfigurationTest")
public class ModuleConfigurationTest {

   public void testModuleConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addModule(MyModuleConfigurationBuilder.class).attribute("testValue");

      Configuration configuration = builder.build();
      assertEquals("testValue", configuration.module(MyModuleConfiguration.class).attribute());

      ConfigurationBuilder secondBuilder = new ConfigurationBuilder();
      secondBuilder.read(configuration, Combine.DEFAULT);
      Configuration secondConfiguration = secondBuilder.build();
      assertEquals("testValue", secondConfiguration.module(MyModuleConfiguration.class).attribute());
   }
}
