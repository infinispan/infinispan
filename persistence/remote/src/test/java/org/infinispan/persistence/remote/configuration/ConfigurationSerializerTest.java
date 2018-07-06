package org.infinispan.persistence.remote.configuration;

import java.nio.file.Paths;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.configuration.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {Paths.get("remote-cl-config-security.xml")}
      };
   }

   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      super.compareStoreConfiguration(name, beforeStore, afterStore);
      RemoteStoreConfiguration before = (RemoteStoreConfiguration) beforeStore;
      RemoteStoreConfiguration after = (RemoteStoreConfiguration) afterStore;
      AssertJUnit.assertEquals("Wrong connection pool for " + name + " configuration.", before.connectionPool(), after.connectionPool());
   }
}
