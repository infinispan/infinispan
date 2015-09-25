package org.infinispan.query.backend;

import org.apache.lucene.document.Document;
import org.hibernate.search.filter.FullTextFilterImplementor;
import org.hibernate.search.spi.BuildContext;
import org.hibernate.search.store.ShardIdentifierProvider;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.CacheManagerService;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Dynamic sharding based on the segment associated with the key
 *
 * @author gustavonalle
 * @since 8.1
 */
public class AffinityShardIdentifierProvider implements ShardIdentifierProvider {

   private KeyTransformationHandler keyTransformationHandler;
   private ComponentRegistry componentRegistry;
   private Set<String> identifiers;

   @Override
   public void initialize(Properties properties, BuildContext buildContext) {
      InfinispanLoopbackService service =
            (InfinispanLoopbackService) buildContext.getServiceManager().requestService(CacheManagerService.class);
      String cacheName = service.getComponentRegistry().getCacheName();
      componentRegistry = service.getComponentRegistry();
      int numSegments = service.getEmbeddedCacheManager().getCacheConfiguration(cacheName).clustering().hash().numSegments();
      keyTransformationHandler = componentRegistry.getComponent(QueryInterceptor.class).getKeyTransformationHandler();
      identifiers = IntStream.rangeClosed(0, numSegments - 1).boxed().map(String::valueOf).collect(Collectors.toSet());
   }

   private ConsistentHash getCH() {
      return componentRegistry.getComponent(DistributionManager.class).getReadConsistentHash();
   }

   private Set<String> getShards() {
      return identifiers;
   }

   @Override
   public String getShardIdentifier(Class<?> entityType, Serializable id, String idAsString, Document document) {
      Object key = keyTransformationHandler.stringToKey(idAsString, null);
      int segment = getCH().getSegment(key);
      return String.valueOf(segment);
   }

   @Override
   public Set<String> getShardIdentifiersForQuery(FullTextFilterImplementor[] fullTextFilters) {
      return getShards();
   }

   @Override
   public Set<String> getAllShardIdentifiers() {
      return getShards();
   }
}
