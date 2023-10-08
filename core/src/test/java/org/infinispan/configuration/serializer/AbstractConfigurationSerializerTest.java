package org.infinispan.configuration.serializer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.configuration.cache.QueryConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.cache.TracingConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class AbstractConfigurationSerializerTest extends AbstractInfinispanTest {
   public static final List<MediaType> ALL = Arrays.asList(MediaType.APPLICATION_XML, MediaType.APPLICATION_YAML, MediaType.APPLICATION_JSON);
   public static final List<MediaType> XML = Collections.singletonList(MediaType.APPLICATION_XML);

   public static class Parameter {
      final Path config;
      final MediaType mediaType;
      final ParserRegistry registry;

      public Parameter(Path config, MediaType mediaType, ParserRegistry registry) {
         this.config = config;
         this.mediaType = mediaType;
         this.registry = registry;
      }

      @Override
      public String toString() {
         return config.subpath(config.getNameCount() - 3, config.getNameCount()) + " (" + mediaType.getSubType().toUpperCase() + ")";
      }
   }

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() throws Exception {
      Path configDir = Paths.get(System.getProperty("build.directory"), "test-classes", "configs", "all");
      Properties properties = new Properties();
      properties.put("jboss.server.temp.dir", System.getProperty("java.io.tmpdir"));
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      List<Path> paths = Files.list(configDir).toList();
      return paths.stream().flatMap(p -> typesFor(p).stream().map(t -> new Object[]{new Parameter(p, t, registry)})).toArray(Object[][]::new);
   }

   public static List<MediaType> typesFor(Path p) {
      try {
         // If the file name starts with a number and it's >= 12, then use all media types. Otherwise just test XML
         return Integer.parseInt(p.getFileName().toString().split("\\.")[0]) >= 12 ? ALL : XML;
      } catch (NumberFormatException e) {
         // The file name is not a version number, test all media types.
         return ALL;
      }
   }

   @Test(dataProvider = "configurationFiles")
   public void configurationSerializationTest(Parameter parameter) throws IOException {
      URL url = FileLookupFactory.newInstance().lookupFileLocation(parameter.config.toString(), Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holderBefore = parameter.registry.parse(url);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Map<String, Configuration> configurations = new LinkedHashMap<>();
      for (Map.Entry<String, ConfigurationBuilder> configuration : holderBefore.getNamedConfigurationBuilders().entrySet()) {
         configurations.put(configuration.getKey(), configuration.getValue().build());
      }
      try (ConfigurationWriter writer = ConfigurationWriter.to(baos).withType(parameter.mediaType).build()) {
         parameter.registry.serialize(writer, holderBefore.getGlobalConfigurationBuilder().build(), configurations);
      }
      log.debug(baos);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ConfigurationBuilderHolder holderAfter = parameter.registry.parse(bais, ConfigurationResourceResolvers.DEFAULT, parameter.mediaType);
      GlobalConfiguration globalConfigurationBefore = holderBefore.getGlobalConfigurationBuilder().build();
      GlobalConfiguration globalConfigurationAfter = holderAfter.getGlobalConfigurationBuilder().build();
      assertEquals(globalConfigurationBefore.security().securityCacheTimeout(), globalConfigurationAfter.security().securityCacheTimeout());
      assertEquals(globalConfigurationBefore.security().securityCacheSize(), globalConfigurationAfter.security().securityCacheSize());
      compareAttributeSets("Global", globalConfigurationBefore.globalState().attributes(), globalConfigurationAfter.globalState().attributes(), "localConfigurationStorage");
      compareAttributeSets("Global", globalConfigurationBefore.jmx().attributes(), globalConfigurationAfter.jmx().attributes(), org.infinispan.configuration.parsing.Attribute.MBEAN_SERVER_LOOKUP.toString());
      compareAttributeSets("Global", globalConfigurationBefore.security().authorization().attributes(), globalConfigurationAfter.security().authorization().attributes());
      compareAttributeSets("Global", globalConfigurationBefore.serialization().attributes(), globalConfigurationAfter.serialization().attributes(), "marshaller", "contextInitializers");
      compareAttributeSets("Global", globalConfigurationBefore.transport().attributes(), globalConfigurationAfter.transport().attributes(), "transport", "properties");
      compareExtraGlobalConfiguration(globalConfigurationBefore, globalConfigurationAfter);

      for (String name : holderBefore.getNamedConfigurationBuilders().keySet()) {
         Configuration configurationBefore = holderBefore.getNamedConfigurationBuilders().get(name).build();
         assertTrue(name, holderAfter.getNamedConfigurationBuilders().containsKey(name));
         Configuration configurationAfter = holderAfter.getNamedConfigurationBuilders().get(name).build();
         compareConfigurations(name, configurationBefore, configurationAfter);
      }
   }

   private void compareConfigurations(String name, Configuration configurationBefore, Configuration configurationAfter) {
      compareAttributeSets(name, configurationBefore.clustering().attributes(), configurationAfter.clustering().attributes());
      compareAttributeSets(name, configurationBefore.clustering().hash().attributes(), configurationAfter.clustering().hash().attributes());
      compareAttributeSets(name, configurationBefore.clustering().l1().attributes(), configurationAfter.clustering().l1().attributes());
      compareAttributeSets(name, configurationBefore.clustering().partitionHandling().attributes(), configurationAfter.clustering().partitionHandling().attributes());
      assertEquals(name, configurationBefore.memory(), configurationAfter.memory());
      compareAttributeSets(name, configurationBefore.expiration().attributes(), configurationAfter.expiration().attributes());
      compareIndexing(name, configurationBefore.indexing(), configurationAfter.indexing());
      compareQuery(name, configurationBefore.query(), configurationAfter.query());
      compareTracing(name, configurationBefore.tracing(), configurationAfter.tracing());
      compareAttributeSets(name, configurationBefore.locking().attributes(), configurationAfter.locking().attributes());
      compareAttributeSets(name, configurationBefore.statistics().attributes(), configurationAfter.statistics().attributes());
      compareAttributeSets(name, configurationBefore.sites().attributes(), configurationAfter.sites().attributes());
      compareSites(name, configurationBefore.sites().allBackups(), configurationAfter.sites().allBackups());
      compareAttributeSets(name, configurationBefore.invocationBatching().attributes(), configurationAfter.invocationBatching().attributes());
      compareAttributeSets(name, configurationBefore.unsafe().attributes(), configurationAfter.unsafe().attributes());
      compareAttributeSets(name, configurationBefore.persistence().attributes(), configurationAfter.persistence().attributes());
      compareStores(name, configurationBefore.persistence().stores(), configurationAfter.persistence().stores());
      compareAttributeSets(name, configurationBefore.security().authorization().attributes(), configurationAfter.security().authorization().attributes());
      compareAttributeSets(name, configurationBefore.transaction().attributes(), configurationAfter.transaction().attributes(), "transaction-manager-lookup");

      compareExtraConfiguration(name, configurationBefore, configurationAfter);
   }

   private void compareQuery(String name, QueryConfiguration before, QueryConfiguration after) {
      assertEquals(String.format("Query attributes for %s mismatch", name), before.attributes(), after.attributes());
   }

   private void compareIndexing(String name, IndexingConfiguration before, IndexingConfiguration after) {
      assertEquals(String.format("Indexing attributes for %s mismatch", name), before.attributes(), after.attributes());
      assertEquals(String.format("Indexing reader for %s mismatch", name), before.reader(), after.reader());
      assertEquals(String.format("Indexing writer for %s mismatch", name), before.writer(), after.writer());
   }

   private void compareTracing(String name, TracingConfiguration before, TracingConfiguration after) {
      assertEquals(String.format("Tracing attributes for %s mismatch", name), before.attributes(), after.attributes());
   }

   protected void compareExtraConfiguration(String name, Configuration configurationBefore, Configuration configurationAfter) {
      // Do nothing. Subclasses can override to implement their own specific comparison
   }

   protected void compareExtraGlobalConfiguration(GlobalConfiguration configurationBefore, GlobalConfiguration configurationAfter) {
      // Do nothing. Subclasses can override to implement their own specific comparison
   }

   private void compareStores(String name, List<StoreConfiguration> beforeStores, List<StoreConfiguration> afterStores) {
      assertEquals("Configuration " + name + " stores count mismatch", beforeStores.size(), afterStores.size());
      for (int i = 0; i < beforeStores.size(); i++) {
         StoreConfiguration beforeStore = beforeStores.get(i);
         StoreConfiguration afterStore = afterStores.get(i);

         compareStoreConfiguration(name, beforeStore, afterStore);
      }
   }

   protected void compareStoreConfiguration(String name, StoreConfiguration beforeStore, StoreConfiguration afterStore) {
      if (beforeStore instanceof AbstractStoreConfiguration) {
         AbstractStoreConfiguration beforeASC = (AbstractStoreConfiguration) beforeStore;
         AbstractStoreConfiguration afterASC = (AbstractStoreConfiguration) afterStore;
         compareAttributeSets(name, beforeASC.attributes(), afterASC.attributes());
         compareAttributeSets(name, beforeASC.async().attributes(), afterASC.async().attributes());
      } else {
         throw new IllegalArgumentException("Cannot compare stores of type: " + beforeStore.getClass().getName());
      }
   }

   protected static void compareAttributeSets(String name, AttributeSet before, AttributeSet after, String... exclude) {
      if (before != null && after != null) {
         List<String> exclusions = exclude != null ? Arrays.asList(exclude) : Collections.emptyList();
         for (Attribute<?> attribute : before.attributes()) {
            if (!exclusions.contains(attribute.name())) {
               assertEquals("Configuration " + name, attribute, after.attribute(attribute.name()));
            }
         }
      }
   }

   private void compareSites(String name, List<BackupConfiguration> sitesBefore, List<BackupConfiguration> sitesAfter) {
      assertEquals("Configuration " + name + " sites count mismatch", sitesBefore.size(), sitesAfter.size());
      for (int i = 0; i < sitesBefore.size(); i++) {
         BackupConfiguration before = sitesBefore.get(i);
         BackupConfiguration after = sitesAfter.get(i);
         assertEquals("Configuration " + name + " stores class mismatch", before.getClass(), after.getClass());
         compareAttributeSets(name, before.attributes(), after.attributes());
         compareAttributeSets(name, before.takeOffline().attributes(), after.takeOffline().attributes());
         compareAttributeSets(name, before.stateTransfer().attributes(), after.stateTransfer().attributes());
      }
   }
}
