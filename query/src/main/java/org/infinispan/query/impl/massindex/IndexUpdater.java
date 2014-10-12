package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.UpdateLuceneWork;
import org.hibernate.search.backend.impl.batch.DefaultBatchBackend;
import org.hibernate.search.bridge.spi.ConversionContext;
import org.hibernate.search.bridge.util.impl.ContextualExceptionBridgeHelper;
import org.hibernate.search.engine.spi.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.DefaultInstanceInitializer;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Handle batch updates to an index.
 *
 * @author gustavonalle
 * @since 7.1
 */
public class IndexUpdater {

   private static final Log LOG = LogFactory.getLog(IndexUpdater.class, Log.class);

   private final SearchIntegrator searchIntegrator;
   private final KeyTransformationHandler keyTransformationHandler;
   private final DefaultBatchBackend defaultBatchBackend;
   private final QueryInterceptor queryInterceptor;

   public IndexUpdater(Cache<?, ?> cache) {
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      this.searchIntegrator = queryInterceptor.getSearchFactory();
      this.keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      DefaultMassIndexerProgressMonitor monitor = new DefaultMassIndexerProgressMonitor(componentRegistry.getTimeService());
      this.defaultBatchBackend = new DefaultBatchBackend(searchIntegrator, monitor);
   }

   public void flush(Class<?> entityType) {
      LOG.flushingIndex(entityType.getName());
      defaultBatchBackend.flush(Util.<Class<?>>asSet(entityType));
   }

   public void purge(Class<?> entityType) {
      LOG.purgingIndex(entityType.getName());
      queryInterceptor.purgeIndex(entityType);
   }

   public void updateIndex(Object key, Object value, String indexName) {
      if (value != null) {
         Class clazz = value.getClass();
         EntityIndexBinding entityIndexBinding = searchIntegrator.getIndexBinding(clazz);
         if (entityIndexBinding == null) {
            // it might be possible to receive not-indexes types
            return;
         }
         ConversionContext conversionContext = new ContextualExceptionBridgeHelper();
         DocumentBuilderIndexedEntity docBuilder = entityIndexBinding.getDocumentBuilder();
         final String idInString = keyTransformationHandler.keyToString(key);
         UpdateLuceneWork updateTask = docBuilder.createUpdateWork(
               clazz,
               value,
               idInString,
               idInString,
               DefaultInstanceInitializer.DEFAULT_INITIALIZER,
               conversionContext
         );
         try {
            IndexManager indexManagerForAddition = entityIndexBinding
                  .getSelectionStrategy().getIndexManagerForAddition(clazz, idInString, idInString, updateTask.getDocument());
            if (indexManagerForAddition.getIndexName().equals(indexName)) {
               defaultBatchBackend.enqueueAsyncWork(updateTask);
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

}
