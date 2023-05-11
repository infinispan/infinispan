package org.infinispan.configuration.parsing;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.configuration.io.NamingStrategy;

/**
 * An enumeration of all the recognized XML element local names, by name.
 *
 * @author Pete Muir
 */
public enum Element {
    // must be first
    UNKNOWN(null),

    // KEEP THESE IN ALPHABETICAL ORDER!

    ADVANCED_EXTERNALIZER,
    ADVANCED_EXTERNALIZERS,
    ALLOW_LIST,
    ASYNC,
    AUTHORIZATION,
    BACKUP,
    BACKUPS,
    BACKUP_FOR,
    BINARY,
    BLOCKING_BOUNDED_QUEUE_THREAD_POOL,
    CACHE_CONTAINER,
    CACHES,
    CACHED_THREAD_POOL,
    CLASS,
    CLUSTERING,
    CLUSTER_LOADER,
    CLUSTER_STORE,
    CLUSTER_PERMISSION_MAPPER,
    CLUSTER_ROLE_MAPPER,
    COMMON_NAME_ROLE_MAPPER,
    COMPATIBILITY,
    CUSTOM_INTERCEPTORS,
    CUSTOM_CONFIGURATION_STORAGE,
    CUSTOM_PERMISSION_MAPPER,
    CUSTOM_ROLE_MAPPER,
    DATA,
    DATA_CONTAINER,
    DEFAULT,
    DISTRIBUTED_CACHE,
    DISTRIBUTED_CACHE_CONFIGURATION,
    ENCODING,
    EVICTION,
    EXPIRATION,
    FILE_STORE,
    GROUPS,
    GROUPER,
    GLOBAL,
    GLOBAL_STATE,
    HASH,
    HEAP,
    JGROUPS,
    IDENTITY_ROLE_MAPPER,
    IMMUTABLE_CONFIGURATION_STORAGE,
    INDEX,
    INDEXED_ENTITIES,
    INDEXED_ENTITY,
    INDEXING,
    INDEX_READER,
    INDEX_SHARDING,
    INDEX_WRITER,
    INDEX_MERGE,
    INTERCEPTOR,
    INVALIDATION_CACHE,
    INVALIDATION_CACHE_CONFIGURATION,
    JMX,
    JMX_STATISTICS,
    KEY_DATA_TYPE("key"),
    KEY_TRANSFORMER,
    KEY_TRANSFORMERS,
    L1,
    LOADER,
    LOCAL_CACHE,
    LOCAL_CACHE_CONFIGURATION,
    PERSISTENCE,
    PERSISTENT_LOCATION,
    LOCKING,
    MANAGED_CONFIGURATION_STORAGE,
    MEMORY,
    METRICS,
    MODULES,
    OBJECT,
    OFF_HEAP,
    NON_BLOCKING_BOUNDED_QUEUE_THREAD_POOL,
    OVERLAY_CONFIGURATION_STORAGE,
    PARTITION_HANDLING,
    PROPERTIES,
    PROPERTY,
    QUERY,
    RECOVERY,
    REGEX,
    REMOTE_SITE,
    REMOTE_SITES,
    REPLICATED_CACHE,
    REPLICATED_CACHE_CONFIGURATION,
    ROLE,
    ROLES,
    ROOT,
    SERIALIZATION_CONTEXT_INITIALIZER("context-initializer"),
    SERIALIZATION_CONTEXT_INITIALIZERS("context-initializers"),
    SHARED_PERSISTENT_LOCATION,
    SCHEDULED_THREAD_POOL,
    SECURITY,
    SERIALIZATION,
    SHUTDOWN,
    SINGLE_FILE_STORE,
    SITE,
    SITES,
    STATE_TRANSFER,
    STACK,
    STACKS,
    STACK_FILE,
    STORE,
    STORE_AS_BINARY,
    STORE_MIGRATOR,
    SYNC,
    TAKE_OFFLINE,
    TEMPORARY_LOCATION,
    THREADS,
    THREAD_FACTORIES,
    THREAD_FACTORY,
    THREAD_POOLS,
    TRANSACTION,
    TRANSPORT,
    UNSAFE,
    VALUE_DATA_TYPE("value"),
    VERSIONING,
    VOLATILE_CONFIGURATION_STORAGE,
    @Deprecated
    WHITE_LIST,
    WRITE_BEHIND,
    ;

   private final String name;

    Element(final String name) {
        this.name = name;
    }

    Element() {
        this.name = NamingStrategy.KEBAB_CASE.convert(name()).toLowerCase();
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    private static final Map<String, Element> MAP;

    static {
        final Map<String, Element> map = new HashMap<>(8);
        for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) map.put(name, element);
        }
        MAP = map;
    }

    public static Element forName(String localName) {
        final Element element = MAP.get(localName);
        return element == null ? UNKNOWN : element;
    }

    public static boolean isCacheElement(String localName) {
        final Element element = forName(localName);
        return element == LOCAL_CACHE
                || element == LOCAL_CACHE_CONFIGURATION
                || element == INVALIDATION_CACHE
                || element == INVALIDATION_CACHE_CONFIGURATION
                || element == REPLICATED_CACHE
                || element == REPLICATED_CACHE_CONFIGURATION
                || element == DISTRIBUTED_CACHE
                || element == DISTRIBUTED_CACHE_CONFIGURATION;
    }

   @Override
   public String toString() {
      return name;
   }
}
