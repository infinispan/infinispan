package org.infinispan.hotrod.configuration;

import static org.infinispan.commons.util.Util.getInstance;
import static org.infinispan.commons.util.Util.loadClass;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.CONFIGURATION;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.FORCE_RETURN_VALUES;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.MARSHALLER;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.MARSHALLER_CLASS;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.NAME;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.TEMPLATE_NAME;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.TRANSACTION_MANAGER;
import static org.infinispan.hotrod.configuration.RemoteCacheConfiguration.TRANSACTION_MODE;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Consumer;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.hotrod.HotRod;
import org.infinispan.hotrod.impl.ConfigurationProperties;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.transaction.lookup.GenericTransactionManagerLookup;

/**
 * Per-cache configuration.
 *
 * @since 14.0
 **/
public class RemoteCacheConfigurationBuilder implements Builder<RemoteCacheConfiguration> {
   private final HotRodConfigurationBuilder builder;
   private final AttributeSet attributes;
   private final NearCacheConfigurationBuilder nearCache;

   RemoteCacheConfigurationBuilder(HotRodConfigurationBuilder builder, String name) {
      this.builder = builder;
      this.attributes = RemoteCacheConfiguration.attributeDefinitionSet();
      this.attributes.attribute(NAME).set(name);
      this.nearCache = new NearCacheConfigurationBuilder(builder);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   RemoteCacheConfigurationBuilder(String name) {
      this(null, name);
   }

   public NearCacheConfigurationBuilder nearCache() {
      return nearCache;
   }

   /**
    * Whether or not to implicitly FORCE_RETURN_VALUE for all calls to this cache.
    */
   public RemoteCacheConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      attributes.attribute(FORCE_RETURN_VALUES).set(forceReturnValues);
      return this;
   }

   /**
    * Specifies the declarative configuration to be used to create the cache if it doesn't already exist on the server.
    *
    * @param configuration the XML representation of a cache configuration.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder configuration(String configuration) {
      attributes.attribute(CONFIGURATION).set(configuration);
      return this;
   }

   /**
    * Specifies a URI pointing to the declarative configuration to be used to create the cache if it doesn't already
    * exist on the server.
    *
    * @param uri the URI of the configuration.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder configurationURI(URI uri) {
      try {
         URL url;
         if (!uri.isAbsolute()) {
            url = FileLookupFactory.newInstance().lookupFileLocation(uri.toString(), this.getClass().getClassLoader());
         } else {
            url = uri.toURL();
         }
         try (Scanner scanner = new Scanner(url.openStream(), StandardCharsets.UTF_8.toString()).useDelimiter("\\A")) {
            return this.configuration(scanner.next());
         }
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }
   }

   /**
    * Specifies the name of a template to be used to create the cache if it doesn't already exist on the server.
    *
    * @param templateName the name of the template.
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder templateName(String templateName) {
      attributes.attribute(TEMPLATE_NAME).set(templateName);
      return this;
   }

   /**
    * The {@link TransactionMode} in which a resource will be enlisted.
    *
    * @param mode the transaction mode
    * @return an instance of the builder
    */
   public RemoteCacheConfigurationBuilder transactionMode(TransactionMode mode) {
      attributes.attribute(TRANSACTION_MODE).set(mode);
      return this;
   }

   /**
    * Specifies a custom {@link Marshaller} implementation. See {@link #marshaller(Marshaller)}.
    *
    * @param className Fully qualifies class name of the marshaller implementation.
    */
   public RemoteCacheConfigurationBuilder marshaller(String className) {
      marshaller(loadClass(className, Thread.currentThread().getContextClassLoader()));
      return this;
   }

   /**
    * Specifies a custom {@link Marshaller} implementation. See {@link #marshaller(Marshaller)}.
    *
    * @param marshallerClass the marshaller class.
    */
   public RemoteCacheConfigurationBuilder marshaller(Class<? extends Marshaller> marshallerClass) {
      attributes.attribute(MARSHALLER_CLASS).set(marshallerClass);
      return this;
   }

   /**
    * Specifies a custom {@link Marshaller} implementation to serialize and deserialize user objects. Has precedence over
    * {@link #marshaller(Class)} and {@link #marshaller(String)}. If not configured, the global marshaller will be used
    * for the cache operations.
    *
    * @param marshaller the marshaller instance
    */
   public RemoteCacheConfigurationBuilder marshaller(Marshaller marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   /**
    * The {@link TransactionManagerLookup} to lookup for the transaction manager to interact with.
    *
    * @param lookup A {@link TransactionManagerLookup} instance.
    * @return An instance of the builder.
    */
   public RemoteCacheConfigurationBuilder transactionManagerLookup(TransactionManagerLookup lookup) {
      attributes.attribute(TRANSACTION_MANAGER).set(lookup);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(CONFIGURATION).isModified() && attributes.attribute(TEMPLATE_NAME).isModified()) {
         throw Log.HOTROD.remoteCacheTemplateNameXorConfiguration(attributes.attribute(NAME).get());
      }
      if (attributes.attribute(TRANSACTION_MODE).get() == null) {
         throw HOTROD.invalidTransactionMode();
      }
      if (attributes.attribute(TRANSACTION_MANAGER).get() == null) {
         throw HOTROD.invalidTransactionManagerLookup();
      }
   }

   @Override
   public RemoteCacheConfiguration create() {
      return new RemoteCacheConfiguration(attributes.protect(), nearCache.create());
   }

   @Override
   public Builder<?> read(RemoteCacheConfiguration template, Combine combine) {
      this.nearCache.read(template.nearCache(), combine);
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   public HotRodConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_CONFIGURATION_SUFFIX, this::configuration);
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_CONFIGURATION_URI_SUFFIX, v -> this.configurationURI(URI.create(v)));
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX, this::templateName);
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_FORCE_RETURN_VALUES_SUFFIX, v -> this.forceReturnValues(Boolean.parseBoolean(v)));
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_TRANSACTION_MODE_SUFFIX, v -> this.transactionMode(TransactionMode.valueOf(v)));
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_TRANSACTION_MANAGER_LOOKUP_SUFFIX, this::transactionManagerLookupClass);
      findCacheProperty(attributes, typed, ConfigurationProperties.CACHE_MARSHALLER, this::marshaller);
      nearCache.withProperties(properties);
      return builder;
   }

   private void transactionManagerLookupClass(String lookupClass) {
      TransactionManagerLookup lookup = lookupClass == null || GenericTransactionManagerLookup.class.getName().equals(lookupClass) ?
            GenericTransactionManagerLookup.getInstance() :
            getInstance(loadClass(lookupClass, HotRod.class.getClassLoader()));
      transactionManagerLookup(lookup);
   }

   private static void findCacheProperty(AttributeSet attributes, TypedProperties properties, String name, Consumer<String> consumer) {
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
