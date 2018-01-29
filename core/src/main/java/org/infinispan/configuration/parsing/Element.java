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

    ADVANCED_EXTERNALIZER("advanced-externalizer"),
    ASYNC("async"),
    AUTHORIZATION("authorization"),
    BACKUP("backup"),
    BACKUPS("backups"),
    BACKUP_FOR("backup-for"),
    BINARY("binary"),
    BLOCKING_BOUNDED_QUEUE_THREAD_POOL("blocking-bounded-queue-thread-pool"),
    CACHE_CONTAINER("cache-container"),
    CACHED_THREAD_POOL("cached-thread-pool"),
    CLUSTERING("clustering"),
    CLUSTER_LOADER("cluster-loader"),
    CLUSTER_STORE("cluster"),
    CLUSTER_ROLE_MAPPER("cluster-role-mapper"),
    COMMON_NAME_ROLE_MAPPER("common-name-role-mapper"),
    COMPATIBILITY("compatibility"),
    CUSTOM_INTERCEPTORS("custom-interceptors"),
    CUSTOM_CONFIGURATION_STORAGE("custom-configuration-storage"),
    CUSTOM_ROLE_MAPPER("custom-role-mapper"),
    DATA_CONTAINER("data-container"),
    DEFAULT("default"),
    DISTRIBUTED_CACHE("distributed-cache"),
    DISTRIBUTED_CACHE_CONFIGURATION("distributed-cache-configuration"),
    ENCODING("encoding"),
    EVICTION("eviction"),
    EXPIRATION("expiration"),
    FILE_STORE("file-store"),
    GROUPS("groups"),
    GROUPER("grouper"),
    GLOBAL("global"),
    GLOBAL_STATE("global-state"),
    HASH("hash"),
    JGROUPS("jgroups"),
    IDENTITY_ROLE_MAPPER("identity-role-mapper"),
    IMMUTABLE_CONFIGURATION_STORAGE("immutable-configuration-storage"),
    INDEXED_ENTITIES("indexed-entities"),
    INDEXED_ENTITY("indexed-entity"),
    INDEXING("indexing"),
    INTERCEPTOR("interceptor"),
    INVALIDATION_CACHE("invalidation-cache"),
    INVALIDATION_CACHE_CONFIGURATION("invalidation-cache-configuration"),
    JMX("jmx"),
    JMX_STATISTICS("jmxStatistics"),
    KEY_DATA_TYPE("key"),
    L1("l1"),
    LOADER("loader"),
    LOCAL_CACHE("local-cache"),
    LOCAL_CACHE_CONFIGURATION("local-cache-configuration"),
    PERSISTENCE("persistence"),
    PERSISTENT_LOCATION("persistent-location"),
    LOCKING("locking"),
    MANAGED_CONFIGURATION_STORAGE("managed-configuration-storage"),
    MEMORY("memory"),
    MODULES("modules"),
    OBJECT("object"),
    OFFHEAP("off-heap"),
    OVERLAY_CONFIGURATION_STORAGE("overlay-configuration-storage"),
    PARTITION_HANDLING("partition-handling"),
    PROPERTIES("properties"),
    PROPERTY("property"),
    RECOVERY("recovery"),
    REPLICATED_CACHE("replicated-cache"),
    REPLICATED_CACHE_CONFIGURATION("replicated-cache-configuration"),
    ROLE("role"),
    ROOT("infinispan"),
    SCATTERED_CACHE("scattered-cache"),
    SCATTERED_CACHE_CONFIGURATION("scattered-cache-configuration"),
    SHARED_PERSISTENT_LOCATION("shared-persistent-location"),
    SCHEDULED_THREAD_POOL("scheduled-thread-pool"),
    SECURITY("security"),
    SERIALIZATION("serialization"),
    SHUTDOWN("shutdown"),
    @Deprecated
    SINGLETON_STORE("singleton"),
    SITE("site"),
    SITES("sites"),
    STATE_TRANSFER("state-transfer"),
    STACK_FILE("stack-file"),
    STORE("store"),
    STORE_AS_BINARY("store-as-binary"),
    SYNC("sync"),
    TAKE_OFFLINE("take-offline"),
    TEMPORARY_LOCATION("temporary-location"),
    THREADS("threads"),
    THREAD_FACTORY("thread-factory"),
    TRANSACTION("transaction"),
    TRANSPORT("transport"),
    UNSAFE("unsafe"),
    VALUE_DATA_TYPE("value"),
    VERSIONING("versioning"),
    VOLATILE_CONFIGURATION_STORAGE("volatile-configuration-storage"),
    WRITE_BEHIND("write-behind"),
    ;

    private final String name;

    Element(final String name) {
        this.name = name;
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
