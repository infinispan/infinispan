package org.infinispan.query.indexmanager;

import static org.infinispan.query.logging.Log.CONTAINER;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Execute a stream operation
 *
 * @author gustavonalle
 * @since 7.0
 */
public class IndexUpdateStreamCommand extends AbstractUpdateCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX_STREAM;

   public IndexUpdateStreamCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      Cache cache = componentRegistry.getCache().wired();
      SearchIntegrator searchFactory = ComponentRegistryUtils.getSearchIntegrator(cache);
      QueryInterceptor queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      if (queryInterceptor.isStopping()) {
         throw CONTAINER.cacheIsStoppingNoCommandAllowed(cacheName.toString());
      }
      IndexManager indexManager = searchFactory.getIndexManager(indexName);
      if (indexManager == null) {
         throw new SearchException("Unknown index referenced : " + indexName);
      }
      List<LuceneWork> luceneWorks = indexManager.getSerializer().toLuceneWorks(this.serializedModel);
      KeyTransformationHandler handler = queryInterceptor.getKeyTransformationHandler();
      LuceneWork workToApply = LuceneWorkConverter.transformKeysToString(luceneWorks.iterator().next(), handler);
      indexManager.performStreamOperation(workToApply, null, true);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }
}
