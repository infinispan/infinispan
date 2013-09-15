package org.infinispan.compatibility.parsing;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public enum Element52 {
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
   CLUSTER_STORE("cluster"),
   COMPATIBILITY("compatibility"),
   CUSTOM_INTERCEPTORS("customInterceptors"),
   DATA_CONTAINER("dataContainer"),
   DEADLOCK_DETECTION("deadlockDetection"),
   DEFAULT("default"),
   EVICTION("eviction"),
   EVICTION_SCHEDULED_EXECUTOR("evictionScheduledExecutor"),
   EXPIRATION("expiration"),
   SINGLE_FILE_STORE("singleFile"),
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
   PERSISTENCE("persistence"),
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
   SINGLETON_STORE("singleton"),
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
   CLUSTER_LOADER("clusterLoader"),
   TAKE_OFFLINE("takeOffline"),
   TOTAL_ORDER_EXECUTOR("totalOrderExecutor"),
   FILE_STORE("fileStore"),
   LOADER("loader"), LOADERS("loaders");

   private final String name;

   Element52(final String name) {
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

   private static final Map<String, Element52> MAP;

   static {
      final Map<String, Element52> map = new HashMap<String, Element52>(8);
      for (Element52 element52 : values()) {
         final String name = element52.getLocalName();
         if (name != null) map.put(name, element52);
      }
      MAP = map;
   }

   public static Element52 forName(String localName) {
      final Element52 element52 = MAP.get(localName);
      return element52 == null ? UNKNOWN : element52;
   }
}
