package org.infinispan.persistence.remote.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.serializer.AbstractConfigurationSerializerTest;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.configuration.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {
   @Override
   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      super.compareStoreConfiguration(name, beforeStore, afterStore);
      RemoteStoreConfiguration before = (RemoteStoreConfiguration) beforeStore;
      RemoteStoreConfiguration after = (RemoteStoreConfiguration) afterStore;
      assertEquals(before.connectionPool(), after.connectionPool(), "Wrong connection pool for " + name + " configuration.");
      assertEquals(before.security(), after.security(), "Wrong security config for " + name + " configuration.");
      assertEquals(before.servers(), after.servers(), "Wrong remote server config for " + name + " configuration.");
   }
}
