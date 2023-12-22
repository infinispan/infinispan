package org.infinispan.embedded;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.infinispan.api.exception.InfinispanException;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.embedded.impl.logging.Log;

/**
 * EmbeddedURI represents an embedded Infinispan configuration. The following URI schemes are supported:
 * <ul>
 *    <li><b>infinispan:file:&lt;path&gt;</b> or <b>file:&lt;path&gt;</b?>points to an Infinispan configuration file (XML, JSON or YAML) on the filesystem</li>
 *    <li><b>infinispan:classpath:&lt;path&gt;</b> or <b>classpath:&lt;path&gt;</b> points to an Infinispan configuration file (XML, JSON or YAML) as a classpath resource</li>
 *    <li><b>&lt;path&gt;</b> looks for an Infinispan configuration file (XML, JSON or YAML) first on the filesystem and then in the classpath</li>
 *    <li><b>infinispan:local://&lt;name&gt;</b> A local (non-clustered) Infinispan instance</li>
 *    <li><b>infinispan:cluster://[username:[password]@][bind]:[port][?propertyName=value&...]</b> A clustered Infinispan instance</li>
 * </ul>
 *
 * @since 15.0
 **/
public class EmbeddedURI {
   private final ConfigurationBuilderHolder configuration;
   private final URI uri;

   public static EmbeddedURI create(String uriString) {
      return create(URI.create(uriString));
   }

   public static EmbeddedURI create(URI uri) {
      if ("file".equals(uri.getScheme())) {
         try {
            return new EmbeddedURI(uri, parseURL(uri.toURL()));
         } catch (IOException e) {
            throw new InfinispanException(e);
         }
      } else if ("classpath".equals(uri.getScheme())) {
         ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
         URL resource = classLoader == null ? null : classLoader.getResource(uri.getPath().substring(1));
         if (resource == null) {
            resource = ClassLoader.getSystemClassLoader().getResource(uri.getPath().substring(1));
         }
         if (resource != null) {
            try {
               return new EmbeddedURI(uri, parseURL(resource));
            } catch (IOException e) {
               throw new InfinispanException(e);
            }
         } else {
            throw new InfinispanException(new FileNotFoundException(uri.toString()));
         }
      } else if ("infinispan".equals(uri.getScheme())) {
         URI subUri = URI.create(uri.getSchemeSpecificPart());
         if ("file".equals(subUri.getScheme()) || "classpath".equals(subUri.getScheme())) {
            return create(subUri);
         }
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         if ("local".equals(subUri.getScheme())) {
            holder.getGlobalConfigurationBuilder().nonClusteredDefault().cacheManagerName(subUri.getHost());
         } else if ("cluster".equals(subUri.getScheme())) {
            holder.getGlobalConfigurationBuilder().clusteredDefault();
         } else {
            throw Log.EMBEDDED.notAnEmbeddedURI(uri.toString());
         }
         return new EmbeddedURI(uri, holder);
      }
      throw Log.EMBEDDED.notAnEmbeddedURI(uri.toString());
   }

   private static ConfigurationBuilderHolder parseURL(URL url) throws IOException {
      try (InputStream is = url.openStream()) {
         ConfigurationReader reader = ConfigurationReader.from(is)
               .withProperties(queryToProperties(url.getQuery()))
               .withType(MediaType.fromExtension(url.getFile()))
               .withResolver(new URLConfigurationResourceResolver(url))
               .build();
         return new ParserRegistry().parse(reader, new ConfigurationBuilderHolder());
      }
   }

   private static Properties queryToProperties(String query) {
      Properties properties = new Properties();
      if (query != null) {
         String[] pairs = query.split("&");
         for (String pair : pairs) {
            int idx = pair.indexOf("=");
            properties.setProperty(
                  URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8),
                  URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
            );
         }
      }
      return properties;
   }

   private EmbeddedURI(URI uri, ConfigurationBuilderHolder configuration) {
      this.uri = uri;
      this.configuration = configuration;
   }

   @Override
   public String toString() {
      return uri.toString();
   }

   ConfigurationBuilderHolder toConfiguration() {
      return configuration;
   }
}
