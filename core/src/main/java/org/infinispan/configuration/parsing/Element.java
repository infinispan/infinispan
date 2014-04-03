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
    ADVANCED_EXTERNALIZERS("advanced-externalizers"),
    ASYNC("async"),
    ASYNC_LISTENER_EXECUTOR("asyncListenerExecutor"),
    ASYNC_TRANSPORT_EXECUTOR("asyncTransportExecutor"),
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
    COMPATIBILITY("compatibility"),
    CUSTOM_INTERCEPTORS("custom-interceptors"),
    DATA_CONTAINER("data-container"),
    DEADLOCK_DETECTION("deadlockDetection"),
    DEFAULT("default"),
    DISTRIBUTED_CACHE("distributed-cache"),
    EVICTION("eviction"),
    EVICTION_SCHEDULED_EXECUTOR("evictionScheduledExecutor"),
    EXPIRATION("expiration"),
    FILE_STORE("file-store"),
    GROUPS("groups"),
    GROUPER("grouper"),
    GLOBAL("global"),
    GLOBAL_JMX_STATISTICS("globalJmxStatistics"),
    HASH("hash"),
    JGROUPS("jgroups"),
    INDEXING("indexing"),
    INTERCEPTOR("interceptor"),
    INVALIDATION_CACHE("invalidation-cache"),
    INVOCATION_BATCHING("invocationBatching"),
    JMX("jmx"),
    JMX_STATISTICS("jmxStatistics"),
    L1("l1"),
    LAZY_DESERIALIZATION("lazyDeserialization"),
    LOADER("loader"),
    LOCAL_CACHE("local-cache"),
    PERSISTENCE("persistence"),
    LOCKING("locking"),
    MODULES("modules"),
    NAMED_CACHE("namedCache"),
    PERSISTENCE_EXECUTOR("persistenceExecutor"),
    PROPERTIES("properties"),
    PROPERTY("property"),
    RECOVERY("recovery"),
    REMOTE_COMMNAND_EXECUTOR("remoteCommandsExecutor"),
    REPLICATED_CACHE("replicated-cache"),
    REPLICATION_QUEUE_SCHEDULED_EXECUTOR("replicationQueueScheduledExecutor"),
    ROLE("role"),
    ROOT("infinispan"),
    SCHEDULED_THREAD_POOL("scheduled-thread-pool"),
    SECURITY("security"),
    SERIALIZATION("serialization"),
    SHUTDOWN("shutdown"),
    SINGLETON_STORE("singleton"),
    SINGLE_FILE_STORE("singleFile"),
    SITE("site"),
    SITES("sites"),
    STATE_RETRIEVAL("stateRetrieval"),
    STATE_TRANSFER("state-transfer"),
    STACK_FILE("stack-file"),
    STORE("store"),
    STORE_AS_BINARY("store-as-binary"),
    SYNC("sync"),
    TAKE_OFFLINE("take-offline"),
    THREADS("threads"),
    THREAD_FACTORY("thread-factory"),
    TOTAL_ORDER_EXECUTOR("totalOrderExecutor"),
    TRANSACTION("transaction"),
    TRANSPORT("transport"),
    UNSAFE("unsafe"),
    VERSIONING("versioning"),
    WRITE_BEHIND("write-behind"),


//   ALIAS(ModelKeys.ALIAS),
//   BACKUP(ModelKeys.BACKUP),
//   BACKUPS(ModelKeys.BACKUPS),
//   BINARY_KEYED_TABLE(ModelKeys.BINARY_KEYED_TABLE),
//   @Deprecated BUCKET_TABLE(ModelKeys.BUCKET_TABLE),
//   CACHE_CONTAINER(ModelKeys.CACHE_CONTAINER),
//   CONNECTION_POOL(ModelKeys.CONNECTION_POOL),
//   CLUSTER_LOADER(ModelKeys.CLUSTER_LOADER),
//   COMPATIBILITY(ModelKeys.COMPATIBILITY),
//   COMPRESSION(ModelKeys.COMPRESSION),
//   DATA_COLUMN(ModelKeys.DATA_COLUMN),
//
//   @Deprecated ENTRY_TABLE(ModelKeys.ENTRY_TABLE),
//   EVICTION(ModelKeys.EVICTION),
//   EXPIRATION(ModelKeys.EXPIRATION),
//   FILE_STORE(ModelKeys.FILE_STORE),
//   ID_COLUMN(ModelKeys.ID_COLUMN),
//
//   LEVELDB_STORE(ModelKeys.LEVELDB_STORE),
//   @Deprecated JDBC_STORE("jdbc-store"),
//   STRING_KEYED_JDBC_STORE(ModelKeys.STRING_KEYED_JDBC_STORE),
//   BINARY_KEYED_JDBC_STORE(ModelKeys.BINARY_KEYED_JDBC_STORE),
//   MIXED_KEYED_JDBC_STORE(ModelKeys.MIXED_KEYED_JDBC_STORE),
//   IMPLEMENTATION(ModelKeys.IMPLEMENTATION),
//   INDEXING(ModelKeys.INDEXING),
//
//   LOCKING(ModelKeys.LOCKING),
//   PROPERTY(ModelKeys.PROPERTY),
//   @Deprecated REHASHING("rehashing"),
//   REMOTE_SERVER(ModelKeys.REMOTE_SERVER),
//   REMOTE_STORE(ModelKeys.REMOTE_STORE),
//
//   REST_STORE(ModelKeys.REST_STORE),
//   STATE_TRANSFER(ModelKeys.STATE_TRANSFER),
//   STORE(ModelKeys.STORE),
//   STRING_KEYED_TABLE(ModelKeys.STRING_KEYED_TABLE),
//   TAKE_OFFLINE(ModelKeys.TAKE_OFFLINE),
//   TIMESTAMP_COLUMN(ModelKeys.TIMESTAMP_COLUMN),
//   TRANSACTION(ModelKeys.TRANSACTION),
//   TRANSPORT(ModelKeys.TRANSPORT),
//
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
