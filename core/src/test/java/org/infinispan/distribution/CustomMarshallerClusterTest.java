package org.infinispan.distribution;

import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.CustomMarshallerClusterTest")
public class CustomMarshallerClusterTest extends MultipleCacheManagersTest {

   private final ConfigurationBuilder cacheConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);

   protected GlobalConfigurationBuilder defaultGlobalConfigurationBuilder() {
      var config = GlobalConfigurationBuilder.defaultClusteredBuilder();
      config.serialization()
            .marshaller(new JavaSerializationMarshaller())
            .allowList().addRegexp(".*");
      return config;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(cacheConfig, 3);
      waitForClusterToForm();
   }

   public void testRemotePutGetWithCollections() {
      var val = new MyClass("v");
      testRemotePutGet(List.of(val));
      testRemotePutGet(List.of(List.of(val)));
      testRemotePutGet(Set.of(val));
      testRemotePutGet(Set.of(Set.of(val)));
   }

   public void testRemotePutGetWithNestedMap() {
      var nestedMapVal = Map.of("parentMap", Map.of("nestedMapKey", new MyClass("v")));
      testRemotePutGet(nestedMapVal);
   }

   private void testRemotePutGet(Object value) {
      var key = new MagicKey(cache(0), cache(1));
      cache(1).put(key, value);
      assertEquals(value, cache(2).get(key));
   }

   record MyClass(String value) implements Serializable {
   }
}
