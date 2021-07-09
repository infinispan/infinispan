package org.infinispan.persistence.remote.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;

/**
 * Utilities to parse and serialize {@link RemoteStoreConfiguration} to and from JSON.
 *
 * @since 13.0
 */
public final class SerializationUtils {
   private static final ParserRegistry parserRegistry = new ParserRegistry();
   private static final String PLACEHOLDER = "cache-holder";

   private SerializationUtils() {
   }

   public static String toJson(RemoteStoreConfiguration configuration) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      RemoteStoreConfigurationBuilder storeBuilder = builder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
      storeBuilder.read(configuration);
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).build()) {
         parserRegistry.serialize(w, null, builder.build());
      }
      return Json.read(sw.toString()).at("local-cache").at("persistence").toString();
   }

   public static RemoteStoreConfiguration fromJson(String json) throws IOException {
      ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
      holder.newConfigurationBuilder(PLACEHOLDER);

      try (ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes(UTF_8))) {
         ConfigurationBuilderHolder parsedHolder = parserRegistry.parse(bais, holder, null, APPLICATION_JSON);
         Configuration parsedConfig = parsedHolder.getNamedConfigurationBuilders().get(PLACEHOLDER).build();
         return (RemoteStoreConfiguration) parsedConfig.persistence().stores().iterator().next();
      }
   }
}
