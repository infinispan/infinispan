package org.infinispan.persistence.remote.configuration;

import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.configuration.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {"remote-cl-config.xml"}
      };
   }
}
