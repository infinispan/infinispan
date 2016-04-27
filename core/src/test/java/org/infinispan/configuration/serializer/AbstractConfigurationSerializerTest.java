package org.infinispan.configuration.serializer;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional")
public abstract class AbstractConfigurationSerializerTest {
   @Test(dataProvider="configurationFiles")
   public void configurationSerializationTest(String config) throws Exception {
      ParserRegistry registry = new ParserRegistry();
      InputStream is = FileLookupFactory.newInstance().lookupFileStrict(config, Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holderBefore = registry.parse(is);
      Map<String, Configuration> configurations = new HashMap<>();
      for(Map.Entry<String, ConfigurationBuilder> configuration : holderBefore.getNamedConfigurationBuilders().entrySet()) {
         configurations.put(configuration.getKey(), configuration.getValue().build());
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      registry.serialize(baos, holderBefore.getGlobalConfigurationBuilder().build(), configurations);

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      try {
         ConfigurationBuilderHolder holderAfter = registry.parse(bais);

         for (String name : holderBefore.getNamedConfigurationBuilders().keySet()) {
            Configuration configurationBefore = holderBefore.getNamedConfigurationBuilders().get(name).build();
            Configuration configurationAfter = holderAfter.getNamedConfigurationBuilders().get(name).build();

            compareAttributeSets(name, configurationBefore.clustering().attributes(), configurationAfter.clustering().attributes());
            compareAttributeSets(name, configurationBefore.compatibility().attributes(), configurationAfter.compatibility().attributes(), "marshaller");
            compareAttributeSets(name, configurationBefore.eviction().attributes(), configurationAfter.eviction().attributes());
            compareAttributeSets(name, configurationBefore.expiration().attributes(), configurationAfter.expiration().attributes());
            compareAttributeSets(name, configurationBefore.indexing().attributes(), configurationAfter.indexing().attributes());
            compareAttributeSets(name, configurationBefore.locking().attributes(), configurationAfter.locking().attributes());
            compareAttributeSets(name, configurationBefore.persistence().attributes(), configurationAfter.persistence().attributes());
            compareStores(name, configurationBefore.persistence().stores(), configurationAfter.persistence().stores());
            compareAttributeSets(name, configurationBefore.security().authorization().attributes(), configurationAfter.security().authorization().attributes());
            compareAttributeSets(name, configurationBefore.storeAsBinary().attributes(), configurationAfter.storeAsBinary().attributes());
            compareAttributeSets(name, configurationBefore.transaction().attributes(), configurationAfter.transaction().attributes(), "transaction-manager-lookup");
            compareAttributeSets(name, configurationBefore.versioning().attributes(), configurationAfter.versioning().attributes());

            compareExtraConfiguration(name, configurationBefore, configurationAfter);
         }
      } catch (Exception e) {
         System.out.println(new String(baos.toByteArray()));
         throw e;
      }
   }

   protected void compareExtraConfiguration(String name, Configuration configurationBefore, Configuration configurationAfter) {
      // Do nothing. Subclasses can override to implement their own specific comparison
   }

   private void compareStores(String name, List<StoreConfiguration> beforeStores, List<StoreConfiguration> afterStores) {
      assertEquals("Configuration "+ name + " stores count mismatch", beforeStores.size(), afterStores.size());
      for(int i = 0; i < beforeStores.size(); i++) {
         StoreConfiguration beforeStore = beforeStores.get(i);
         StoreConfiguration afterStore = afterStores.get(i);
         assertEquals("Configuration " + name + " stores class mismatch", beforeStore.getClass(), afterStore.getClass());
         compareStoreConfiguration(name, beforeStore, afterStore);
      }
   }

   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      if (beforeStore instanceof AbstractStoreConfiguration) {
         AbstractStoreConfiguration beforeASC = (AbstractStoreConfiguration) beforeStore;
         AbstractStoreConfiguration afterASC = (AbstractStoreConfiguration) afterStore;
         compareAttributeSets(name, beforeASC.attributes(), afterASC.attributes());
         compareAttributeSets(name, beforeASC.singletonStore().attributes(), afterASC.singletonStore().attributes());
         compareAttributeSets(name, beforeASC.async().attributes(), afterASC.async().attributes());
      } else {
         throw new IllegalArgumentException("Cannot compare stores of type: "+beforeStore.getClass().getName());
      }
   }

   protected void compareAttributeSets(String name, AttributeSet before, AttributeSet after, String... exclude) {
      List<String> exclusions = exclude != null ? Arrays.asList(exclude) : Collections.emptyList();
      for(Attribute<?> attribute : before.attributes()) {
         if (!exclusions.contains(attribute.name()))
            assertEquals("Configuration "+name, attribute, after.attribute(attribute.name()));
      }
   }
}
