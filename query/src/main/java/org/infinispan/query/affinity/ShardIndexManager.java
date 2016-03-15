package org.infinispan.query.affinity;

import org.apache.lucene.search.similarities.Similarity;
import org.hibernate.search.engine.service.spi.ServiceManager;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.indexes.spi.DirectoryBasedIndexManager;
import org.hibernate.search.spi.WorkerBuildContext;
import org.hibernate.search.store.DirectoryProvider;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.query.backend.ComponentRegistryService;

import java.util.Properties;

/**
 * {@link org.hibernate.search.indexes.spi.IndexManager} that splits the index into shards.
 *
 * @author gustavonalle
 * @since 8.2
 */
public class ShardIndexManager extends DirectoryBasedIndexManager {

   @Override
   public EntityIndexBinding getIndexBinding(Class<?> entityType) {
      return super.getIndexBinding(entityType);
   }

   @Override
   public void initialize(String indexName, Properties properties, Similarity similarity, WorkerBuildContext buildContext) {
      super.initialize(indexName, properties, similarity, buildContext);
      ServiceManager serviceManager = buildContext.getServiceManager();
      ComponentRegistryService componentRegistryService = serviceManager.requestService(ComponentRegistryService.class);
      ComponentRegistry componentRegistry = componentRegistryService.getComponentRegistry();
      Cache cache = componentRegistry.getComponent(Cache.class);
      cache.addListener(new TopologyChangeListener());
      flushAndReleaseResources();
   }

   @Override
   protected DirectoryProvider<?> createDirectoryProvider(String indexName, Properties cfg, WorkerBuildContext buildContext) {
      String shardName = indexName.substring(indexName.lastIndexOf(".") + 1);
      InfinispanDirectoryProvider directoryProvider = new InfinispanDirectoryProvider(Integer.valueOf(shardName));
      directoryProvider.initialize(indexName, cfg, buildContext);

      return directoryProvider;
   }

   @Listener
   public class TopologyChangeListener {

      public TopologyChangeListener() {
      }

      @TopologyChanged
      @SuppressWarnings("unused")
      public void onTopologyChange(TopologyChangedEvent<?, ?> tce) {
         if (!tce.isPre()) {
            flushAndReleaseResources();
         }
      }
   }
}
