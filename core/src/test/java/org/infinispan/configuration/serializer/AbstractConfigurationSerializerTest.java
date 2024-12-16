package org.infinispan.configuration.serializer;

import static org.infinispan.configuration.serializer.ConfigurationSerializerValidator.compareAttributeSets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class AbstractConfigurationSerializerTest extends AbstractInfinispanTest {
   public static final List<MediaType> ALL = Arrays.asList(MediaType.APPLICATION_XML, MediaType.APPLICATION_YAML, MediaType.APPLICATION_JSON);
   public static final List<MediaType> XML = Collections.singletonList(MediaType.APPLICATION_XML);

   private final ConfigurationSerializerValidator validator = new ConfigurationSerializerValidator(
         this::compareExtraConfiguration,
         this::compareExtraGlobalConfiguration,
         this::compareStoreConfiguration
   );

   @DataProvider(name = "configurationFiles")
   public Object[][] configurationFiles() throws Exception {
      Path configDir = Paths.get(System.getProperty("build.directory"), "test-classes", "configs", "all");
      Properties properties = new Properties();
      properties.put("jboss.server.temp.dir", System.getProperty("java.io.tmpdir"));
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      List<Path> paths = Files.list(configDir).toList();
      return paths.stream().flatMap(p -> typesFor(p).stream().map(t -> new Object[]{
            new ConfigurationSerializerValidator.Parameter(p, t, registry)})).toArray(Object[][]::new);
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
   public void configurationSerializationTest(ConfigurationSerializerValidator.Parameter parameter) throws IOException {
      validator.validateConfigurationSerialization(parameter);
   }

   protected void compareExtraConfiguration(String name, Configuration configurationBefore, Configuration configurationAfter) {
      // Do nothing. Subclasses can override to implement their own specific comparison
   }

   protected void compareExtraGlobalConfiguration(GlobalConfiguration configurationBefore, GlobalConfiguration configurationAfter) {
      // Do nothing. Subclasses can override to implement their own specific comparison
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
}
