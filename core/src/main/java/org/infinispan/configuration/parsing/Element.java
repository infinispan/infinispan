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
    CUSTOM_ROLE_MAPPER("custom-role-mapper"),
    DATA_CONTAINER("data-container"),
    DEFAULT("default"),
    DISTRIBUTED_CACHE("distributed-cache"),
    EVICTION("eviction"),
    EXPIRATION("expiration"),
    FILE_STORE("file-store"),
    GROUPS("groups"),
    GROUPER("grouper"),
    GLOBAL("global"),
    HASH("hash"),
    JGROUPS("jgroups"),
    IDENTITY_ROLE_MAPPER("identity-role-mapper"),
    INDEXING("indexing"),
    INTERCEPTOR("interceptor"),
    INVALIDATION_CACHE("invalidation-cache"),
    JMX("jmx"),
    JMX_STATISTICS("jmxStatistics"),
    L1("l1"),
    LOADER("loader"),
    LOCAL_CACHE("local-cache"),
    PERSISTENCE("persistence"),
    LOCKING("locking"),
    MODULES("modules"),
    PROPERTIES("properties"),
    PROPERTY("property"),
    RECOVERY("recovery"),
    REPLICATED_CACHE("replicated-cache"),
    ROLE("role"),
    ROOT("infinispan"),
    SCHEDULED_THREAD_POOL("scheduled-thread-pool"),
    SECURITY("security"),
    SERIALIZATION("serialization"),
    SHUTDOWN("shutdown"),
    SINGLETON_STORE("singleton"),
    SITE("site"),
    SITES("sites"),
    STATE_TRANSFER("state-transfer"),
    STACK_FILE("stack-file"),
    STORE("store"),
    STORE_AS_BINARY("store-as-binary"),
    SYNC("sync"),
    TAKE_OFFLINE("take-offline"),
    THREADS("threads"),
    THREAD_FACTORY("thread-factory"),
    TRANSACTION("transaction"),
    TRANSPORT("transport"),
    UNSAFE("unsafe"),
    VERSIONING("versioning"),
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
        final Map<String, Element> map = new HashMap<String, Element>(8);
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
}
