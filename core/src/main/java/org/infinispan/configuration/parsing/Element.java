package org.infinispan.configuration.parsing;

import java.util.HashMap;
import java.util.Map;

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
    ASYNC,
    AUTHORIZATION,
    BACKUP,
    BACKUPS,
    BACKUP_FOR,
    BINARY,
    BLOCKING_BOUNDED_QUEUE_THREAD_POOL,
    CACHE_CONTAINER,
    CACHED_THREAD_POOL,
    CLUSTERING,
    CLUSTER_LOADER,
    CLUSTER_STORE,
    CLUSTER_ROLE_MAPPER,
    COMMON_NAME_ROLE_MAPPER,
    COMPATIBILITY,
    CUSTOM_INTERCEPTORS,
    CUSTOM_CONFIGURATION_STORAGE,
    CUSTOM_ROLE_MAPPER,
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
    JGROUPS,
    IDENTITY_ROLE_MAPPER,
    IMMUTABLE_CONFIGURATION_STORAGE,
    INDEXED_ENTITIES,
    INDEXED_ENTITY,
    INDEXING,
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
    MODULES,
    OBJECT,
    OFF_HEAP,
    OVERLAY_CONFIGURATION_STORAGE,
    PARTITION_HANDLING,
    PROPERTIES,
    PROPERTY,
    RECOVERY,
    REMOTE_SITE,
    REMOTE_SITES,
    REPLICATED_CACHE,
    REPLICATED_CACHE_CONFIGURATION,
    ROLE,
    ROOT,
    SCATTERED_CACHE,
    SCATTERED_CACHE_CONFIGURATION,
    SHARED_PERSISTENT_LOCATION,
    SCHEDULED_THREAD_POOL,
    SECURITY,
    SERIALIZATION,
    SHUTDOWN,
    @Deprecated
    SINGLETON_STORE("singleton"),
    SITE,
    SITES,
    STATE_TRANSFER,
    STACK,
    STACK_FILE,
    STORE,
    STORE_AS_BINARY,
    SYNC,
    TAKE_OFFLINE,
    TEMPORARY_LOCATION,
    THREADS,
    THREAD_FACTORY,
    TRANSACTION,
    TRANSPORT,
    UNSAFE,
    VALUE_DATA_TYPE("value"),
    VERSIONING,
    VOLATILE_CONFIGURATION_STORAGE,
    WRITE_BEHIND,
    ;

   private final String name;

    Element(final String name) {
        this.name = name;
    }

    Element() {
        this.name = name().toLowerCase().replace('_', '-');
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

   @Override
   public String toString() {
      return name;
   }
}
