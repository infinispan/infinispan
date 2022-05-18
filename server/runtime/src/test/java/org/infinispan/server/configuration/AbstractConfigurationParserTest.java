package org.infinispan.server.configuration;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.server.Server;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class AbstractConfigurationParserTest {

   protected final MediaType type;

   protected AbstractConfigurationParserTest(MediaType type) {
      this.type = type;
   }

   @Parameterized.Parameters(name = "{0}")
   public static Iterable<MediaType> mediaTypes() {
      return Arrays.asList(MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_YAML);
   }

   protected ServerConfiguration loadAndParseConfiguration() throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL url = fileLookup.lookupFileLocation(path(), this.getClass().getClassLoader());
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      properties.setProperty(Server.INFINISPAN_SERVER_HOME_PATH, Paths.get(System.getProperty("build.directory")).toString());
      ParserRegistry registry = new ParserRegistry(this.getClass().getClassLoader(), false, properties);

      ConfigurationBuilderHolder holder = registry.parse(url);
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      return global.module(ServerConfiguration.class);
   }

   public static Path getConfigPath() {
      return Paths.get(System.getProperty("build.directory"), "test-classes", "configuration");
   }

   protected abstract String path();
}
