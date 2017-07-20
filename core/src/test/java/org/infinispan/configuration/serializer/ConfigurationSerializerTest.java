package org.infinispan.configuration.serializer;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "configuration.serializer.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {"configs/unified/7.0.xml"},
            {"configs/unified/7.1.xml"},
            {"configs/unified/7.2.xml"},
            {"configs/unified/8.0.xml"},
            {"configs/unified/8.1.xml"},
            {"configs/unified/8.2.xml"},
            {"configs/unified/9.0.xml"},
            {"configs/unified/9.1.xml"},
            {"configs/unified/9.2.xml"}
      };
   }
}
