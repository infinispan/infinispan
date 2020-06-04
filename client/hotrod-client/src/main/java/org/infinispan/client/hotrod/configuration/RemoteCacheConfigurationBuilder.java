package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.CONFIGURATION;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.NAME;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.NEAR_CACHE_MAX_ENTRIES;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.NEAR_CACHE_MODE;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.TEMPLATE_NAME;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.TRANSACTION_MANAGER;
import static org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration.TRANSACTION_MODE;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Consumer;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

/**
 * Per-cache configuration.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class RemoteCacheConfigurationBuilder implements Builder<RemoteCacheConfiguration> {
   private final ConfigurationBuilder builder;
   private final AttributeSet attributes;

   RemoteCacheConfigurationBuilder(ConfigurationBuilder builder, String name) {
      this.builder = builder;
      this.attributes = RemoteCacheConfiguration.attributeDefinitionSet();
      this.attributes.attribute(NAME).set(name);
   }

   /**
    * Whether or not to implicitly FORCE_RETURN_VALUE for all calls to this cache.
    */
   public RemoteCacheConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      attributes.attribute(FORCE_RETURN_VALUES).set(forceReturnValues);
      return this;
   }

   /**
    * Specifies the near caching mode. See {@link NearCacheMode} for details on the available modes.
    *
    * @param mode one of {@link NearCacheMode}
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder nearCacheMode(NearCacheMode mode) {
      attributes.attribute(NEAR_CACHE_MODE).set(mode);
      return this;
   }

   /**
    * Specifies the maximum number of entries that will be held in the near cache. Only works when {@link #nearCacheMode(NearCacheMode)} is not {@link NearCacheMode#DISABLED}.
    *
    * @param maxEntries maximum entries in the near cache.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder nearCacheMaxEntries(int maxEntries) {
      attributes.attribute(NEAR_CACHE_MAX_ENTRIES).set(maxEntries);
      return this;
   }

   /**
    * Specifies the declarative configuration to be used to create the cache if it doesn't already exist on the server.
    * @param configuration the XML representation of a cache configuration.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder configuration(String configuration) {
      attributes.attribute(CONFIGURATION).set(configuration);
      return this;
   }

   /**
    * Specifies a URI pointing to the declarative configuration to be used to create the cache if it doesn't already exist on the server.
    * @param uri the URI of the configuration.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder configurationURI(URI uri) {
      try (Scanner scanner = new Scanner(uri.toURL().openStream(), StandardCharsets.UTF_8.toString()).useDelimiter("\\A")) {
         return this.configuration(scanner.next());
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   /**
    * Specifies the name of a template to be used to create the cache if it doesn't already exist on the server.
    * @param templateName the name of the template.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder templateName(String templateName) {
      attributes.attribute(TEMPLATE_NAME).set(templateName);
      return this;
   }

   /**
    * Specifies one of the default templates to be used to create the cache if it doesn't already exist on the server.
    * @param template the template to use
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder templateName(DefaultTemplate template) {
      attributes.attribute(TEMPLATE_NAME).set(template.getTemplateName());
      return this;
   }

   /**
    * The {@link TransactionMode} in which a {@link RemoteCache} will be enlisted.
    * @param mode the transaction mode
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder transactionMode(TransactionMode mode) {
      attributes.attribute(TRANSACTION_MODE).set(mode);
      return this;
   }

   /**
    * The {@link javax.transaction.TransactionManager} to use for the cache
    * @param manager an instance of a TransactionManager
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder transactionManager(TransactionManager manager) {
      attributes.attribute(TRANSACTION_MANAGER).set(manager);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(CONFIGURATION).isModified() && attributes.attribute(TEMPLATE_NAME).isModified()) {
         throw Log.HOTROD.remoteCacheTemplateNameXorConfiguration(attributes.attribute(NAME).get());
      }
   }

   @Override
   public RemoteCacheConfiguration create() {
      return new RemoteCacheConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(RemoteCacheConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      findCacheProperty(typed, ConfigurationProperties.CACHE_CONFIGURATION_SUFFIX, v -> this.configuration(v));
      findCacheProperty(typed, ConfigurationProperties.CACHE_CONFIGURATION_URI_SUFFIX, v -> this.configurationURI(URI.create(v)));
      findCacheProperty(typed, ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX, v -> this.templateName(v));
      findCacheProperty(typed, ConfigurationProperties.CACHE_FORCE_RETURN_VALUES_SUFFIX, v -> this.forceReturnValues(Boolean.parseBoolean(v)));
      findCacheProperty(typed, ConfigurationProperties.CACHE_NEAR_CACHE_MODE_SUFFIX, v -> this.nearCacheMode(NearCacheMode.valueOf(v)));
      findCacheProperty(typed, ConfigurationProperties.CACHE_NEAR_CACHE_MAX_ENTRIES_SUFFIX, v -> this.nearCacheMaxEntries(Integer.parseInt(v)));
      findCacheProperty(typed, ConfigurationProperties.CACHE_TRANSACTION_MODE_SUFFIX, v -> this.transactionMode(TransactionMode.valueOf(v)));

      return builder;
   }

   private void findCacheProperty(TypedProperties properties, String name, Consumer<String> consumer) {
      String cacheName = attributes.attribute(NAME).get();
      String value = null;
      if (properties.containsKey(ConfigurationProperties.CACHE_PREFIX + cacheName + name)) {
         value = properties.getProperty(ConfigurationProperties.CACHE_PREFIX + cacheName + name, true);
      } else if (properties.containsKey(ConfigurationProperties.CACHE_PREFIX + '[' + cacheName + ']' + name)) {
         value = properties.getProperty(ConfigurationProperties.CACHE_PREFIX + '[' + cacheName + ']' + name, true);
      }
      if (value != null) {
         consumer.accept(value);
      }
   }
}
