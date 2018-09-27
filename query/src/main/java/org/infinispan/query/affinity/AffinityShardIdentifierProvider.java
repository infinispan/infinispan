package org.infinispan.query.affinity;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.hibernate.search.filter.FullTextFilterImplementor;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.store.ShardIdentifierProvider;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.backend.ComponentRegistryService;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;


/**
 * Dynamic sharding based on the Infinispan CH segment associated with the key.
 *
 * @author gustavonalle
 * @since 8.2
 */
public class AffinityShardIdentifierProvider implements ShardIdentifierProvider {

   private static final Log log = LogFactory.getLog(AffinityShardIdentifierProvider.class, Log.class);

   public static final int DEFAULT_NUMBER_SHARDS = 4;
   public static final String NUMBER_OF_SHARDS_PROP = "nbr_of_shards";
   public static final String SHARDING_STRATEGY_PROP = "sharding_strategy";

   // these are lazily initialized from ComponentRegistry
   private Address address;
   private DistributionManager distributionManager;
   private KeyTransformationHandler keyTransformationHandler;
   private ComponentRegistry componentRegistry;
   private ShardAllocatorManager shardAllocatorManager;

   @Override
   public void initialize(Properties properties, BuildContext buildContext) {
      ComponentRegistryService componentRegistryService = buildContext.getServiceManager().requestService(ComponentRegistryService.class);
      componentRegistry = componentRegistryService.getComponentRegistry();
      RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
      if (rpcManager != null) {
         address = rpcManager.getAddress();
         distributionManager = componentRegistry.getComponent(DistributionManager.class);
      } else {
         address = LocalModeAddress.INSTANCE;
      }
      Configuration configuration = componentRegistry.getComponent(Configuration.class);
      int numSegments = configuration.clustering().hash().numSegments();
      int numberOfShards = getNumberOfShards(properties);
      shardAllocatorManager = componentRegistry.getComponent(ShardAllocatorManager.class);
      shardAllocatorManager.initialize(numberOfShards, numSegments);
      if (log.isDebugEnabled()) {
         log.debugf("Initialized AffinityShardIdentifierProvider with %d shards", numberOfShards);
      }
   }

   private int getSegment(Object key) {
      return distributionManager == null ? 0 : distributionManager.getCacheTopology().getSegment(key);
   }

   @Override
   public String getShardIdentifier(Class<?> entityType, Serializable id, String idAsString, Document document) {
      Object key = getKeyTransformationHandler().stringToKey(idAsString);
      int segment = getSegment(key);
      String shardId = shardAllocatorManager.getShardFromSegment(segment);
      if (log.isDebugEnabled()) {
         log.debugf("Shard Identifier for segment[%s] = %d mapped to shard %s", id, segment, shardId);
      }
      return shardId;
   }

   @Override
   public Set<String> getShardIdentifiersForQuery(FullTextFilterImplementor[] fullTextFilters) {
      return shardAllocatorManager.getShards();
   }

   @Override
   public Set<String> getShardIdentifiersForDeletion(Class<?> entity, Serializable id, String idInString) {
      Set<String> shardsForModification = shardAllocatorManager.getShardsForModification(address);
      if (shardsForModification == null) {
         shardsForModification = Collections.emptySet();
      }
      if (log.isDebugEnabled()) {
         log.debugf("Shard for modification, [%d] %s", shardsForModification.size(), shardsForModification);
      }
      return shardsForModification;
   }

   @Override
   public Set<String> getAllShardIdentifiers() {
      return shardAllocatorManager.getShards();
   }

   private KeyTransformationHandler getKeyTransformationHandler() {
      if (keyTransformationHandler == null) {
         keyTransformationHandler = componentRegistry.getComponent(QueryInterceptor.class)
               .getKeyTransformationHandler();
      }
      return keyTransformationHandler;
   }

   private static int getNumberOfShards(Properties properties) {
      String nShards = properties.getProperty(NUMBER_OF_SHARDS_PROP);
      if (nShards != null) return Integer.parseInt(nShards);

      nShards = properties.getProperty(SHARDING_STRATEGY_PROP + "." + NUMBER_OF_SHARDS_PROP);
      return nShards != null ? Integer.parseInt(nShards) : DEFAULT_NUMBER_SHARDS;
   }
}
