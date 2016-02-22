package org.infinispan.query.affinity;

import org.apache.lucene.document.Document;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.filter.FullTextFilterImplementor;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.store.ShardIdentifierProvider;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.CacheManagerService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.backend.ComponentRegistryService;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.remoting.rpc.RpcManager;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Dynamic sharding based on the segment associated with the key
 *
 * @author gustavonalle
 * @since 8.2
 */
public class AffinityShardIdentifierProvider implements ShardIdentifierProvider {

   // these are lazily initialized from ComponentRegistry
   private RpcManager rpcManager;
   private DistributionManager distributionManager;
   private KeyTransformationHandler keyTransformationHandler;
   private ComponentRegistry componentRegistry;
   private Set<String> identifiers;

   @Override
   public void initialize(Properties properties, BuildContext buildContext) {
      ServiceManager serviceManager = buildContext.getServiceManager();
      ComponentRegistryService componentRegistryService = serviceManager.requestService(ComponentRegistryService.class);
      this.componentRegistry = componentRegistryService.getComponentRegistry();
      CacheManagerService cacheManagerService = serviceManager.requestService(CacheManagerService.class);
      EmbeddedCacheManager embeddedCacheManager = cacheManagerService.getEmbeddedCacheManager();
      String cacheName = componentRegistry.getCacheName();
      ClusteringConfiguration clusteringConfiguration = embeddedCacheManager.getCacheConfiguration(cacheName).clustering();
      int numSegments = clusteringConfiguration.cacheMode().isClustered() ? clusteringConfiguration.hash().numSegments() : 1;
      identifiers = IntStream.rangeClosed(0, numSegments - 1).boxed().map(String::valueOf).collect(Collectors.toSet());
   }

   private int getSegment(Object key) {
      DistributionManager distributionManager = getDistributionManager();
      if (distributionManager == null) {
         return 0;
      }
      return distributionManager.getReadConsistentHash().getSegment(key);
   }

   private Set<String> getShards() {
      return identifiers;
   }

   @Override
   public String getShardIdentifier(Class<?> entityType, Serializable id, String idAsString, Document document) {
      Object key = getKeyTransformationHandler().stringToKey(idAsString, null);
      int segment = getSegment(key);
      return String.valueOf(segment);
   }

   @Override
   public Set<String> getShardIdentifiersForQuery(FullTextFilterImplementor[] fullTextFilters) {
      return getShards();
   }

   @Override
   public Set<String> getShardIdentifiersForDeletion(Class<?> entity, Serializable id, String idInString) {
      if (getDistributionManager() == null) {
         return Collections.singleton("0");
      }
      Set<Integer> segmentsForOwner = getDistributionManager().getConsistentHash().getPrimarySegmentsForOwner(getRpcManager().getAddress());
      return segmentsForOwner.stream().map(String::valueOf).collect(Collectors.toSet());
   }

   @Override
   public Set<String> getAllShardIdentifiers() {
      return getShards();
   }

   private KeyTransformationHandler getKeyTransformationHandler() {
      if (keyTransformationHandler == null) {
         keyTransformationHandler = componentRegistry.getComponent(QueryInterceptor.class).getKeyTransformationHandler();
      }
      return keyTransformationHandler;
   }

   private RpcManager getRpcManager() {
      if (rpcManager == null) {
         rpcManager = componentRegistry.getComponent(RpcManager.class);
      }
      return rpcManager;
   }

   private DistributionManager getDistributionManager() {
      if (distributionManager == null) {
         distributionManager = componentRegistry.getComponent(DistributionManager.class);
      }
      return distributionManager;
   }
}
