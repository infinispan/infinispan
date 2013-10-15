package org.infinispan.persistence.cli.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "unit", testName = "persistence.remote.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testCacheLoaderConfiguration() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.persistence().addStore(CLInterfaceLoaderConfigurationBuilder.class)
         .connectionString("jmx://1.2.3.4:4444/MyCacheManager/myCache");
      Configuration configuration = b.build();
      CLInterfaceLoaderConfiguration store = (CLInterfaceLoaderConfiguration)
            configuration.persistence().stores().get(0);
      assertEquals("jmx://1.2.3.4:4444/MyCacheManager/myCache", store.connectionString());
   }

}