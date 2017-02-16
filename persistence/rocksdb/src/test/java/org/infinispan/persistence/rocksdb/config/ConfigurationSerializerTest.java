package org.infinispan.persistence.rocksdb.config;

import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.rocksdb.configuration.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {"config/rocksdb-config.xml"},
      };
   }
}
