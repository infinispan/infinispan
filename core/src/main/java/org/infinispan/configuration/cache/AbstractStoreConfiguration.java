package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.Element;

public class AbstractStoreConfiguration<T extends StoreConfiguration> extends ConfigurationElement<AbstractStoreConfiguration<T>> implements StoreConfiguration {
   public static final AttributeDefinition<Boolean> PURGE_ON_STARTUP = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PURGE, false).immutable().build();
   public static final AttributeDefinition<Boolean> READ_ONLY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.READ_ONLY, false).immutable().build();
   public static final AttributeDefinition<Boolean> WRITE_ONLY = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.WRITE_ONLY, false).immutable().build();
   public static final AttributeDefinition<Boolean> PRELOAD = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PRELOAD, false).immutable().build();
   public static final AttributeDefinition<Boolean> SHARED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SHARED, false).immutable().build();
   public static final AttributeDefinition<Boolean> TRANSACTIONAL = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TRANSACTIONAL, false).immutable().build();
   public static final AttributeDefinition<Integer> MAX_BATCH_SIZE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MAX_BATCH_SIZE, 100).immutable().build();
   public static final AttributeDefinition<Boolean> SEGMENTED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SEGMENTED, true).immutable().build();
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder(Element.PROPERTIES, null, TypedProperties.class)
         .initializer(() -> new TypedProperties()).autoPersist(false).immutable().build();
   private final Attribute<Integer> maxBatchSize;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AbstractStoreConfiguration.class, PURGE_ON_STARTUP,
            READ_ONLY, WRITE_ONLY, PRELOAD, SHARED, TRANSACTIONAL, MAX_BATCH_SIZE, SEGMENTED, PROPERTIES);
   }

   private final AsyncStoreConfiguration async;

   protected AbstractStoreConfiguration(Enum<?> element, AttributeSet attributes, AsyncStoreConfiguration async, ConfigurationElement<?>... children) {
      super(element, attributes, Util.concat(children, async));
      this.async = async;
      maxBatchSize = attributes.attribute(MAX_BATCH_SIZE);
   }

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous writes to the cache
    * store, giving you 'write-behind' caching.
    */
   @Override
   public AsyncStoreConfiguration async() {
      return async;
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public boolean purgeOnStartup() {
      return attributes.attribute(PURGE_ON_STARTUP).get();
   }

   @Override
   public boolean shared() {
      return attributes.attribute(SHARED).get();
   }

   @Override
   public boolean transactional() {
      return attributes.attribute(TRANSACTIONAL).get();
   }

   @Override
   public int maxBatchSize() {
      return maxBatchSize.get();
   }

   @Override
   public boolean segmented() {
      return attributes.attribute(SEGMENTED).get();
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained, only one of them can
    * have this property enabled. Persistent state transfer with a shared cache store does not make sense, as the same
    * persistent store that provides the data will just end up receiving it. Therefore, if a shared cache store is used,
    * the cache will not allow a persistent state transfer even if a cache store has this property set to true.
    *
    * @deprecated since 14.0. Always returns false. The first non shared store is used instead.
    */
   @Deprecated
   @Override
   public boolean fetchPersistentState() {
      return false;
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be applied to the cache
    * store. This means that the cache store could become out of sync with the cache.
    */
   @Override
   public boolean ignoreModifications() {
      return attributes.attribute(READ_ONLY).get();
   }

   /**
    * If true, any operation that reads from the cache won't be retrieved from the given store. This includes bulk
    * operations as well.
    */
   @Override
   public boolean writeOnly() {
      return attributes.attribute(WRITE_ONLY).get();
   }

   @Override
   public boolean preload() {
      return attributes.attribute(PRELOAD).get();
   }

   @Override
   public Properties properties() {
      return attributes.attribute(PROPERTIES).get();
   }
}
