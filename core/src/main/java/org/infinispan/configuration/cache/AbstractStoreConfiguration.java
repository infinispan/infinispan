package org.infinispan.configuration.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

public class AbstractStoreConfiguration implements StoreConfiguration, ConfigurationInfo {
   public static final AttributeDefinition<Boolean> FETCH_PERSISTENT_STATE = AttributeDefinition.builder("fetchPersistentState", false).xmlName("fetch-state").immutable().build();
   public static final AttributeDefinition<Boolean> PURGE_ON_STARTUP = AttributeDefinition.builder("purgeOnStartup", false).immutable().xmlName("purge").build();
   public static final AttributeDefinition<Boolean> IGNORE_MODIFICATIONS = AttributeDefinition.builder("ignoreModifications", false).immutable().xmlName("read-only").build();
   public static final AttributeDefinition<Boolean> PRELOAD = AttributeDefinition.builder("preload", false).immutable().build();
   public static final AttributeDefinition<Boolean> SHARED = AttributeDefinition.builder("shared", false).immutable().build();
   public static final AttributeDefinition<Boolean> TRANSACTIONAL = AttributeDefinition.builder("transactional", false).immutable().build();
   public static final AttributeDefinition<Integer> MAX_BATCH_SIZE = AttributeDefinition.builder("maxBatchSize", 100).immutable().build();
   public static final AttributeDefinition<Boolean> SEGMENTED = AttributeDefinition.builder("segmented", false).immutable().build();
   public static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition.builder("properties", null, TypedProperties.class)
         .initializer(() -> new TypedProperties()).autoPersist(false).immutable().build();

   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AbstractStoreConfiguration.class, FETCH_PERSISTENT_STATE, PURGE_ON_STARTUP,
            IGNORE_MODIFICATIONS, PRELOAD, SHARED, TRANSACTIONAL, MAX_BATCH_SIZE, SEGMENTED, PROPERTIES);
   }

   private final Attribute<Boolean> fetchPersistentState;
   private final Attribute<Boolean> purgeOnStartup;
   private final Attribute<Boolean> ignoreModifications;
   private final Attribute<Boolean> preload;
   private final Attribute<Boolean> shared;
   private final Attribute<Boolean> transactional;
   private final Attribute<Integer> maxBatchSize;
   private final Attribute<Boolean> segmented;
   private final Attribute<TypedProperties> properties;

   protected final AttributeSet attributes;
   private final AsyncStoreConfiguration async;
   private final SingletonStoreConfiguration singletonStore;

   /**
    * @deprecated Use {@link AbstractStoreConfiguration#AbstractStoreConfiguration(AttributeSet, AsyncStoreConfiguration, SingletonStoreConfiguration) instead
    */
   @Deprecated
   public AbstractStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                     AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties) {
      attributes = attributeDefinitionSet();
      attributes.attribute(PURGE_ON_STARTUP).set(purgeOnStartup);
      attributes.attribute(FETCH_PERSISTENT_STATE).set(fetchPersistentState);
      attributes.attribute(IGNORE_MODIFICATIONS).set(ignoreModifications);
      attributes.attribute(PRELOAD).set(preload);
      attributes.attribute(SHARED).set(shared);
      attributes.attribute(TRANSACTIONAL).set(false);
      attributes.attribute(SEGMENTED).set(false);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));

      this.async = async;
      this.singletonStore = singletonStore;
      this.fetchPersistentState = attributes.attribute(FETCH_PERSISTENT_STATE);
      this.purgeOnStartup = attributes.attribute(PURGE_ON_STARTUP);
      this.ignoreModifications = attributes.attribute(IGNORE_MODIFICATIONS);
      this.preload = attributes.attribute(PRELOAD);
      this.shared = attributes.attribute(SHARED);
      this.transactional = attributes.attribute(TRANSACTIONAL);
      this.maxBatchSize = attributes.attribute(MAX_BATCH_SIZE);
      this.segmented = attributes.attribute(SEGMENTED);
      this.properties = attributes.attribute(PROPERTIES);
      this.subElements.addAll(Arrays.asList(async, singletonStore));
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public AbstractStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
                                     SingletonStoreConfiguration singletonStore) {
      this.attributes = attributes.checkProtection();
      this.async = async;
      this.singletonStore = singletonStore;
      this.fetchPersistentState = attributes.attribute(FETCH_PERSISTENT_STATE);
      this.purgeOnStartup = attributes.attribute(PURGE_ON_STARTUP);
      this.ignoreModifications = attributes.attribute(IGNORE_MODIFICATIONS);
      this.preload = attributes.attribute(PRELOAD);
      this.shared = attributes.attribute(SHARED);
      this.transactional = attributes.attribute(TRANSACTIONAL);
      this.maxBatchSize = attributes.attribute(MAX_BATCH_SIZE);
      this.segmented = attributes.attribute(SEGMENTED);
      this.properties = attributes.attribute(PROPERTIES);
      this.subElements.addAll(Arrays.asList(async, singletonStore));
   }

   /**
    * Configuration for the async cache loader. If enabled, this provides you with asynchronous
    * writes to the cache store, giving you 'write-behind' caching.
    */
   @Override
   public AsyncStoreConfiguration async() {
      return async;
   }

   /**
    * SingletonStore is a delegating store used for situations when only one instance in a cluster
    * should interact with the underlying store. The coordinator of the cluster will be responsible
    * for the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore
    * implementation. It always delegates reads to the real CacheStore.
    */
   @Override
   public SingletonStoreConfiguration singletonStore() {
      return singletonStore;
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   @Override
   public boolean purgeOnStartup() {
      return purgeOnStartup.get();
   }

   @Override
   public boolean shared() {
      return shared.get();
   }

   @Override
   public boolean transactional() {
      return transactional.get();
   }

   @Override
   public int maxBatchSize() {
      return maxBatchSize.get();
   }

   @Override
   public boolean segmented() {
      return segmented.get();
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true.
    */
   @Override
   public boolean fetchPersistentState() {
      return fetchPersistentState.get();
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   @Override
   public boolean ignoreModifications() {
      return ignoreModifications.get();
   }

   @Override
   public boolean preload() {
      return preload.get();
   }

   @Override
   public Properties properties() {
      return properties.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "AbstractStoreConfiguration [attributes=" + attributes + ", async=" + async + ", singletonStore="
            + singletonStore + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((async == null) ? 0 : async.hashCode());
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      result = prime * result + ((singletonStore == null) ? 0 : singletonStore.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AbstractStoreConfiguration other = (AbstractStoreConfiguration) obj;
      if (async == null) {
         if (other.async != null)
            return false;
      } else if (!async.equals(other.async))
         return false;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      if (singletonStore == null) {
         if (other.singletonStore != null)
            return false;
      } else if (!singletonStore.equals(other.singletonStore))
         return false;
      return true;
   }
}
