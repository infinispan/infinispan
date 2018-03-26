package org.infinispan.hibernate.cache.spi;

import org.infinispan.manager.EmbeddedCacheManager;

public interface InfinispanProperties {

   String PREFIX = "hibernate.cache.infinispan.";

   String CONFIG_SUFFIX = ".cfg";

   String DEPRECATED_STRATEGY_SUFFIX = ".eviction.strategy";

   String SIZE_SUFFIX = ".memory.size";

   // The attribute was incorrectly named; in fact this sets expiration check interval
   // (eviction is triggered by writes, expiration is time-based)
   String DEPRECATED_WAKE_UP_INTERVAL_SUFFIX = ".eviction.wake_up_interval";

   String DEPRECATED_MAX_ENTRIES_SUFFIX = ".eviction.max_entries";

   String WAKE_UP_INTERVAL_SUFFIX = ".expiration.wake_up_interval";

   String LIFESPAN_SUFFIX = ".expiration.lifespan";

   String MAX_IDLE_SUFFIX = ".expiration.max_idle";

   String ENTITY = "entity";
   String NATURAL_ID = "naturalid";
   String COLLECTION = "collection";
   String IMMUTABLE_ENTITY = "immutable-entity";
   String TIMESTAMPS = "timestamps";
   String QUERY = "query";
   String PENDING_PUTS = "pending-puts";

   /**
    * Classpath or filesystem resource containing Infinispan configurations the factory should use.
    *
    * @see #DEF_INFINISPAN_CONFIG_RESOURCE
    */
   String INFINISPAN_CONFIG_RESOURCE_PROP = "hibernate.cache.infinispan.cfg";

   /**
    * Property name that controls whether Infinispan statistics are enabled.
    * The property value is expected to be a boolean true or false, and it
    * overrides statistic configuration in base Infinispan configuration,
    * if provided.
    */
   String INFINISPAN_GLOBAL_STATISTICS_PROP = "hibernate.cache.infinispan.statistics";

   /**
    * Property that controls whether Infinispan should interact with the
    * transaction manager as a {@link javax.transaction.Synchronization} or as
    * an XA resource.
    * @deprecated Infinispan Second Level Cache is designed to always register as synchronization
    *             on transactional caches, or use non-transactional caches.
    *
    * @see #DEF_USE_SYNCHRONIZATION
    */
   @Deprecated
   String INFINISPAN_USE_SYNCHRONIZATION_PROP = "hibernate.cache.infinispan.use_synchronization";

   /**
    * Name of the configuration that should be used for natural id caches.
    *
    * @see #DEF_ENTITY_RESOURCE
    */
   @SuppressWarnings("UnusedDeclaration")
   String NATURAL_ID_CACHE_RESOURCE_PROP = PREFIX + NATURAL_ID + CONFIG_SUFFIX;

   /**
    * Name of the configuration that should be used for entity caches.
    *
    * @see #DEF_ENTITY_RESOURCE
    */
   @SuppressWarnings("UnusedDeclaration")
   String ENTITY_CACHE_RESOURCE_PROP = PREFIX + ENTITY + CONFIG_SUFFIX;

   /**
    * Name of the configuration that should be used for immutable entity caches.
    * Defaults to the same configuration as {@link #ENTITY_CACHE_RESOURCE_PROP} - {@link #DEF_ENTITY_RESOURCE}
    */
   @SuppressWarnings("UnusedDeclaration")
   String IMMUTABLE_ENTITY_CACHE_RESOURCE_PROP = PREFIX + IMMUTABLE_ENTITY + CONFIG_SUFFIX;

   /**
    * Name of the configuration that should be used for collection caches.
    * No default value, as by default we try to use the same Infinispan cache
    * instance we use for entity caching.
    *
    * @see #ENTITY_CACHE_RESOURCE_PROP
    * @see #DEF_ENTITY_RESOURCE
    */
   @SuppressWarnings("UnusedDeclaration")
   String COLLECTION_CACHE_RESOURCE_PROP = PREFIX + COLLECTION + CONFIG_SUFFIX;

   /**
    * Name of the configuration that should be used for timestamp caches.
    *
    * @see #DEF_TIMESTAMPS_RESOURCE
    */
   @SuppressWarnings("UnusedDeclaration")
   String TIMESTAMPS_CACHE_RESOURCE_PROP = PREFIX + TIMESTAMPS + CONFIG_SUFFIX;

   /**
    * Name of the configuration that should be used for query caches.
    *
    * @see #DEF_QUERY_RESOURCE
    */
   String QUERY_CACHE_RESOURCE_PROP = PREFIX + QUERY + CONFIG_SUFFIX;

   /**
    * Name of the configuration that should be used for pending-puts caches.
    *
    * @see #DEF_PENDING_PUTS_RESOURCE
    */
   @SuppressWarnings("UnusedDeclaration")
   String PENDING_PUTS_CACHE_RESOURCE_PROP = PREFIX + PENDING_PUTS + CONFIG_SUFFIX;

   /**
    * Default value for {@link #INFINISPAN_CONFIG_RESOURCE_PROP}. Specifies the "infinispan-configs.xml" file in this package.
    */
   String DEF_INFINISPAN_CONFIG_RESOURCE = "org/infinispan/hibernate/cache/commons/builder/infinispan-configs.xml";

   /**
    * Default configuration for cases where non-clustered cache manager is provided.
    */
   String INFINISPAN_CONFIG_LOCAL_RESOURCE = "org/infinispan/hibernate/cache/commons/builder/infinispan-configs-local.xml";

   /**
    * Default value for {@link #ENTITY_CACHE_RESOURCE_PROP}.
    */
   String DEF_ENTITY_RESOURCE = "entity";

   /**
    * Default value for {@link #TIMESTAMPS_CACHE_RESOURCE_PROP}.
    */
   String DEF_TIMESTAMPS_RESOURCE = "timestamps";

   /**
    * Default value for {@link #QUERY_CACHE_RESOURCE_PROP}.
    */
   String DEF_QUERY_RESOURCE = "local-query";

   /**
    * Default value for {@link #PENDING_PUTS_CACHE_RESOURCE_PROP}
    */
   String DEF_PENDING_PUTS_RESOURCE = "pending-puts";

   /**
    * @deprecated Use {@link #DEF_PENDING_PUTS_RESOURCE} instead.
    */
   @Deprecated
   String PENDING_PUTS_CACHE_NAME = DEF_PENDING_PUTS_RESOURCE;

   /**
    * Default value for {@link #INFINISPAN_USE_SYNCHRONIZATION_PROP}.
    */
   boolean DEF_USE_SYNCHRONIZATION = true;

   /**
    * Specifies the JNDI name under which the {@link EmbeddedCacheManager} to use is bound.
    * There is no default value -- the user must specify the property.
    */
   String CACHE_MANAGER_RESOURCE_PROP = "hibernate.cache.infinispan.cachemanager";
}
