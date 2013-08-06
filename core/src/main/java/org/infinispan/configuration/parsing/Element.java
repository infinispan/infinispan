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

    //
    ADVANCED_EXTERNALIZER("advancedExternalizer"),
    ADVANCED_EXTERNALIZERS("advancedExternalizers"),
    ASYNC("async"),
    ASYNC_LISTENER_EXECUTOR("asyncListenerExecutor"),
    ASYNC_TRANSPORT_EXECUTOR("asyncTransportExecutor"),
    REMOTE_COMMNAND_EXECUTOR("remoteCommandsExecutor"),
    CLUSTERING("clustering"),
    CLUSTER_LOADER("clusterLoader"),
    COMPATIBILITY("compatibility"),
    CUSTOM_INTERCEPTORS("customInterceptors"),
    DATA_CONTAINER("dataContainer"),
    DEADLOCK_DETECTION("deadlockDetection"),
    DEFAULT("default"),
    EVICTION("eviction"),
    EVICTION_SCHEDULED_EXECUTOR("evictionScheduledExecutor"),
    EXPIRATION("expiration"),
    FILE_STORE("fileStore"),
    SINGLE_FILE_STORE("singleFileStore"),
    BRNO_FILE_STORE("brnoFileStore"),
    GROUPS("groups"),
    GROUPER("grouper"),
    GLOBAL("global"),
    GLOBAL_JMX_STATISTICS("globalJmxStatistics"),
    HASH("hash"),
    INDEXING("indexing"),
    INTERCEPTOR("interceptor"),
    INVOCATION_BATCHING("invocationBatching"),
    JMX_STATISTICS("jmxStatistics"),
    L1("l1"),
    LAZY_DESERIALIZATION("lazyDeserialization"),
    LOADER("loader"),
    LOADERS("loaders"),
    LOCKING("locking"),
    MODULES("modules"),
    NAMED_CACHE("namedCache"),
    PROPERTIES("properties"),
    PROPERTY("property"),
    RECOVERY("recovery"),
    REPLICATION_QUEUE_SCHEDULED_EXECUTOR("replicationQueueScheduledExecutor"),
    ROOT("infinispan"),
    SERIALIZATION("serialization"),
    SHUTDOWN("shutdown"),
    SINGLETON_STORE("singletonStore"),
    STATE_RETRIEVAL("stateRetrieval"),
    STATE_TRANSFER("stateTransfer"),
    STORE("store"),
    STORE_AS_BINARY("storeAsBinary"),
    SYNC("sync"),
    TRANSACTION("transaction"),
    TRANSPORT("transport"),
    UNSAFE("unsafe"),
    VERSIONING("versioning"),
    SITES("sites"),
    SITE("site"),
    BACKUPS("backups"),
    BACKUP("backup"),
    BACKUP_FOR("backupFor"),
    TAKE_OFFLINE("takeOffline"),
    TOTAL_ORDER_EXECUTOR("totalOrderExecutor"),
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
