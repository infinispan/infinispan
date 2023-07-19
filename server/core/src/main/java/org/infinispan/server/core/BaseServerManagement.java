package org.infinispan.server.core;

import static org.infinispan.stats.impl.LocalContainerStatsImpl.LOCAL_CONTAINER_STATS;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.metrics.impl.BaseOperatingSystemAdditionalMetrics;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.stats.ClusterContainerStats;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;

public abstract class BaseServerManagement implements ServerManagement {

   private static final String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";

   @Override
   public Json overviewReport() {
      DefaultCacheManager cacheManager = getCacheManager();
      GlobalComponentRegistry registry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      PersistentUUIDManager uuidManager = registry.getComponent(PersistentUUIDManager.class);
      ClusterContainerStats containerStats = registry.getComponent(ClusterContainerStats.class, LOCAL_CONTAINER_STATS);

      Address nodeAddress = cacheManager.getAddress();
      Address coordinatorAddress = cacheManager.getCoordinator();
      String clusterName = cacheManager.getClusterName();

      String nodeId = (nodeAddress == null) ? null : uuidManager.getPersistentUuid(nodeAddress).toString();
      String coordinatorId = (coordinatorAddress == null) ? null : uuidManager.getPersistentUuid(coordinatorAddress).toString();

      Json clusterMembers = Json.array();
      boolean showClusterMembers = true;
      if (cacheManager.getTransport() != null) {
         for (Address address : cacheManager.getTransport().getMembers()) {
            PersistentUUID uuid = uuidManager.getPersistentUuid(address);
            if (uuid == null) {
               showClusterMembers = false;
               break;
            } else {
               clusterMembers.add(uuid.toString());
            }
         }
      } else {
         clusterMembers.add("local");
      }

      HashSet<String> cacheStores = new HashSet<>();
      HashSet<String> encodings = new HashSet<>();
      CacheManagerInfo info = cacheManager.getCacheManagerInfo();
      Map<String, Integer> cacheFeatures = info.getCacheNames().map(name ->
                  SecurityActions.getCacheConfiguration(cacheManager, name))
            .map(configuration -> {
               collectCacheStores(cacheStores, configuration);
               collectEncodings(encodings, configuration);
               return cacheFeatures(configuration);
            })
            .sorted()
            .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum, LinkedHashMap::new));

      Json features = Json.object();
      int numberOfCaches = 0;
      for (Map.Entry<String, Integer> feature : cacheFeatures.entrySet()) {
         features.set(feature.getKey(), feature.getValue());
         numberOfCaches += feature.getValue();
      }

      Json memory = Json.object()
            .set("available", containerStats != null ? containerStats.getMemoryAvailable() : -1)
            .set("max", containerStats != null ? containerStats.getMemoryMax() : -1)
            .set("total", containerStats != null ? containerStats.getMemoryTotal() : -1)
            .set("used", containerStats != null ? containerStats.getMemoryUsed() : -1);

      Json cpu = new BaseOperatingSystemAdditionalMetrics().cpuReport();

      Json thread = null;
      try {
         ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
         thread = Json.object("count", threadMXBean.getThreadCount())
               .set("daemon-count", threadMXBean.getDaemonThreadCount())
               .set("max-count", threadMXBean.getPeakThreadCount())
               .set("total-started", threadMXBean.getTotalStartedThreadCount());
      } catch (Error error) {
         // An error will be thrown for unsupported operations
         // e.g. SubstrateVM does not support getAllThreadIds
      }

      Set<String> cacheNames = cacheManager.getCacheNames();
      int protoSchemas = (!cacheNames.contains(PROTOBUF_METADATA_CACHE_NAME)) ? 0 :
            SecurityActions.getUnwrappedCache(cacheManager, PROTOBUF_METADATA_CACHE_NAME).size();

      ServerStateManager serverStateManager = getServerStateManager();
      Json clients = (serverStateManager == null) ? Json.nil() : serverStateManager.clientsReport();

      Json report = Json.object("version", "1.0.0")
              .set("node-id", nodeId)
              .set("coordinator-id", coordinatorId)
              .set("cluster-name", clusterName)
              .set("cluster-size", info.getClusterSize());

      if (showClusterMembers) {
         report.set("cluster-members", clusterMembers);
      }

      report = report
            .set("number-of-caches", numberOfCaches)
            .set("cache-features", features)
            .set("cache-configurations", Json.make(info.externalCacheConfigurations()))
            .set("local-site", info.getLocalSite())
            .set("relay-node", info.isRelayNode())
            .set("sites-view", Json.make(info.getSites()))
            .set("memory", memory)
            .set("cpu", cpu);

      if (thread != null) {
         report.set("thread", thread);
      }

      return report
            .set("proto-schemas", protoSchemas)
            .set("cache-stores", cacheStores)
            .set("used-encodings", encodings)
            .set("clients", clients)
            .set("security", securityOverviewReport());
   }

   private static void collectCacheStores(HashSet<String> cacheStores, Configuration configuration) {
      for (StoreConfiguration store : configuration.persistence().stores()) {
         if (store instanceof ConfigurationElement) {
            String elementName = ((ConfigurationElement<?>) store).elementName();
            cacheStores.add(elementName);
         }
      }
   }

   private static void collectEncodings(HashSet<String> encodings, Configuration configuration) {
      EncodingConfiguration encoding = configuration.encoding();

      Attribute<MediaType> mediaTypeAttribute = encoding.attributes().attribute("media-type");
      if (mediaTypeAttribute != null) {
         MediaType mediaType = mediaTypeAttribute.get();
         if (mediaType != null) {
            encodings.add(mediaType.toString());
         }
      }

      MediaType keyMediaType = encoding.keyDataType().mediaType();
      if (keyMediaType != null) {
         encodings.add(keyMediaType.toString());
      }

      MediaType valueDataType = encoding.valueDataType().mediaType();
      if (valueDataType != null) {
         encodings.add(valueDataType.toString());
      }
   }

   private static String cacheFeatures(Configuration configuration) {
      List<String> features = new ArrayList<>(7);
      if (configuration.simpleCache()) {
         features.add("simpleCache");
      }
      if (configuration.transaction().transactionMode().isTransactional()) {
         features.add("transactional");
      }
      if (configuration.persistence().usingStores()) {
         features.add("persistence");
      }
      if (configuration.memory().whenFull().isEnabled()) {
         features.add("bounded");
      }
      if (configuration.security().authorization().enabled()) {
         features.add("secured");
      }
      if (configuration.indexing().enabled()) {
         features.add("indexed");
      }
      if (configuration.sites().hasBackups()) {
         features.add("hasRemoteBackup");
      }
      return (features.isEmpty()) ? "no-features" : String.join("+", features);
   }
}
