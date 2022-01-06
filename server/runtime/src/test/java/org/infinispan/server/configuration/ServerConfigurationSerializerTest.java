package org.infinispan.server.configuration;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.server.Server;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @since 14.0
 **/
@RunWith(Parameterized.class)
public class ServerConfigurationSerializerTest {
   static final List<MediaType> types = Arrays.asList(MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON, MediaType.APPLICATION_YAML);
   static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final Path config;
   private final MediaType type;

   @Parameterized.Parameters(name = "{0} > [{2}]")
   public static Iterable<Object[]> data() throws IOException {
      Path configDir = Paths.get(System.getProperty("build.directory"), "test-classes", "configuration", "versions");
      ServerConfigurationParserTest.createCredentialStore(configDir.getParent().resolve("credentials.pfx"), "secret");
      return Files.list(configDir).flatMap(p -> types.stream().map(t -> new Object[]{p.getFileName(), p, t})).collect(Collectors.toList());
   }

   public ServerConfigurationSerializerTest(Path name, Path config, MediaType type) {
      this.config = config;
      this.type = type;
   }

   @Test
   public void testConfigurationSerialization() throws IOException {
      Properties properties = new Properties();
      properties.put("infinispan.server.config.path", config.getParent().getParent().toString());
      properties.setProperty(Server.INFINISPAN_SERVER_HOME_PATH, Paths.get(System.getProperty("build.directory")).toString());
      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), false, properties);
      ConfigurationBuilderHolder holderBefore = registry.parse(config);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Map<String, Configuration> configurations = new HashMap<>();
      for (Map.Entry<String, ConfigurationBuilder> configuration : holderBefore.getNamedConfigurationBuilders().entrySet()) {
         configurations.put(configuration.getKey(), configuration.getValue().build());
      }
      try (ConfigurationWriter writer = ConfigurationWriter.to(baos).withType(type).clearTextSecrets(true).build()) {
         registry.serialize(writer, holderBefore.getGlobalConfigurationBuilder().build(), configurations);
      }
      log.debug(baos);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ConfigurationBuilderHolder holderAfter = registry.parse(bais, null, type);
      GlobalConfiguration globalConfigurationBefore = holderBefore.getGlobalConfigurationBuilder().build();
      GlobalConfiguration globalConfigurationAfter = holderAfter.getGlobalConfigurationBuilder().build();
      ServerConfiguration serverBefore = globalConfigurationBefore.module(ServerConfiguration.class);
      ServerConfiguration serverAfter = globalConfigurationAfter.module(ServerConfiguration.class);
      compare(serverBefore.interfaces.interfaces(), serverAfter.interfaces.interfaces());
      compare(serverBefore.socketBindings, serverAfter.socketBindings);
      compare(serverBefore.dataSources, serverAfter.dataSources);
      compare(serverBefore.security.credentialStores(), serverAfter.security.credentialStores());
      compare(serverBefore.security.realms().realms(), serverAfter.security.realms().realms());
      compare(serverBefore.security.transport(), serverAfter.security.transport(), org.infinispan.server.configuration.Attribute.SECURITY_REALM.toString());
      compare(serverBefore.endpoints.endpoints(), serverAfter.endpoints.endpoints());
   }

   <T extends ConfigurationElement> void compare(List<T> before, List<T> after) {
      assertEquals(before.size(), after.size());
      for (Iterator<T> b = before.iterator(), a = after.iterator(); b.hasNext(); ) {
         compare(b.next(), a.next());
      }
   }

   <T extends ConfigurationElement> void compare(Map<String, T> before, Map<String, T> after) {
      assertEquals(before.size(), after.size());
      for (Iterator<T> b = before.values().iterator(), a = after.values().iterator(); b.hasNext(); ) {
         compare(b.next(), a.next());
      }
   }

   static void compare(ConfigurationElement<?> before, ConfigurationElement<?> after, String... exclude) {
      assertEquals(before.elementName(), after.elementName());
      compare(before.elementName(), before.attributes(), after.attributes(), exclude);
      assertEquals(before.children().length, after.children().length);
      for (int i = 0; i < before.children().length; i++) {
         compare(before.children()[i], after.children()[i]);
      }
   }

   static void compare(String name, AttributeSet before, AttributeSet after, String... exclude) {
      if (before != null && after != null) {
         List<String> exclusions = exclude != null ? Arrays.asList(exclude) : Collections.emptyList();
         for (Attribute<?> attribute : before.attributes()) {
            if (!exclusions.contains(attribute.name())) {
               assertEquals("Configuration " + name, attribute, after.attribute(attribute.name()));
            }
         }
      }
   }
}
