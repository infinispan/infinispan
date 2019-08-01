package org.infinispan.persistence.sifs;

import java.nio.file.Paths;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(testName = "persistence.sifs.configuration.ConfigurationSerializerTest", groups="functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() {
      return new Object[][] {
            {Paths.get("sifs-config.xml")}
      };
   }

   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      super.compareStoreConfiguration(name, beforeStore, afterStore);
      SoftIndexFileStoreConfiguration before = (SoftIndexFileStoreConfiguration) beforeStore;
      SoftIndexFileStoreConfiguration after = (SoftIndexFileStoreConfiguration) afterStore;
      AssertJUnit.assertEquals("Wrong data config for " + name + " configuration.", before.data(), after.data());
      AssertJUnit.assertEquals("Wrong index config for " + name + " configuration.", before.index(), after.index());
   }
}
