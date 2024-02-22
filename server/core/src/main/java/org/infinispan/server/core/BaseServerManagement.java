package org.infinispan.server.core;

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
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.topology.PersistentUUIDManager;

public abstract class BaseServerManagement implements ServerManagement {

   private static final String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";

   @Override
   public Json overviewReport() {
      DefaultCacheManager cacheManager = getCacheManager();
      GlobalComponentRegistry registry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      PersistentUUIDManager uuidManager = registry.getComponent(PersistentUUIDManager.class);

      Address nodeAddress = cacheManager.getAddress();
      Address coordinatorAddress = cacheManager.getCoordinator();

      String nodeId = (nodeAddress == null) ? null : uuidManager.getPersistentUuid(nodeAddress).toString();
      String coordinatorId = (coordinatorAddress == null) ? null : uuidManager.getPersistentUuid(coordinatorAddress).toString();

      HashSet<String> cacheStores = new HashSet<>();
      HashSet<String> encodings = new HashSet<>();
      CacheManagerInfo info = cacheManager.getCacheManagerInfo();
      Json cacheCardinalities = Json.array();
      Map<String, Integer> cacheFeatures = info.getCacheNames().filter(name -> {
         int size;
            try {
               size = cacheManager.getCache(name).size();
            } catch (Throwable throwable) {
               return false;
            }
            cacheCardinalities.add(cacheCardinality(size));
            return true;
         })
         .map(name -> SecurityActions.getCacheConfiguration(cacheManager, name))
         .map(configuration -> {
            collectCacheStores(cacheStores, configuration);
            collectEncodings(encodings, configuration);
            return cacheFeatures(configuration);
         })
         .sorted()
         .collect(Collectors.toMap(s -> s, s -> 1, Integer::sum, LinkedHashMap::new));

      Json features = Json.object();
      for (Map.Entry<String, Integer> feature : cacheFeatures.entrySet()) {
         features.set(feature.getKey(), feature.getValue());
      }

      Set<String> cacheNames = cacheManager.getCacheNames();
      int protoSchemas = (!cacheNames.contains(PROTOBUF_METADATA_CACHE_NAME)) ? 0 :
            SecurityActions.getUnwrappedCache(cacheManager, PROTOBUF_METADATA_CACHE_NAME).size();

      ServerStateManager serverStateManager = getServerStateManager();
      Json clients = (serverStateManager == null) ? Json.nil() : serverStateManager.clientsReport();

      return Json.object("version", "1.0.0")
            .set("product-version", Version.getBrandVersion())
            .set("node-id", nodeId)
            .set("coordinator-id", coordinatorId)
            .set("cluster-size", info.getClusterSize())
            .set("sites", info.getSites().size())
            .set("relay-node", info.isRelayNode())
            .set("cache-cardinalities", cacheCardinalities)
            .set("cache-features", features)
            .set("cache-stores", cacheStores)
            .set("proto-schemas", protoSchemas)
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

   private static Json cacheCardinality(int size) {
      return Json.make("KMGTPEZ".charAt((int) (Math.log10(size + 1) / 3)) + "");
   }
}
