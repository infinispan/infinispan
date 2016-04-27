package org.infinispan.persistence.leveldb.config;

import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.leveldb.configuration.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {"config/leveldb-config-auto.xml"},
            {"config/leveldb-config-java.xml"},
            {"config/leveldb-config-jni.xml"},
      };
   }
}
