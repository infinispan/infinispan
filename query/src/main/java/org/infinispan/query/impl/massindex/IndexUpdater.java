package org.infinispan.query.impl.massindex;

import org.hibernate.search.backend.UpdateLuceneWork;
import org.hibernate.search.bridge.spi.ConversionContext;
import org.hibernate.search.bridge.util.impl.ContextualExceptionBridgeHelper;
import org.hibernate.search.engine.spi.DocumentBuilderIndexedEntity;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.DefaultInstanceInitializer;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.commons.time.TimeService;
import org.infinispan.query.backend.KeyTransformationHandler;
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
   private final ExtendedBatchBackend defaultBatchBackend;

   public IndexUpdater(SearchIntegrator searchIntegrator, KeyTransformationHandler keyTransformationHandler, TimeService timeService) {
      this.searchIntegrator = searchIntegrator;
      this.keyTransformationHandler = keyTransformationHandler;
      this.defaultBatchBackend = new ExtendedBatchBackend(searchIntegrator, new DefaultMassIndexerProgressMonitor(timeService));
   }

   public void flush(IndexedTypeIdentifier entity) {
      LOG.flushingIndex(entity.getName());
      defaultBatchBackend.flush(entity.asTypeSet());
   }

   public void purge(IndexedTypeIdentifier entity) {
      LOG.purgingIndex(entity.getName());
      defaultBatchBackend.purge(entity.asTypeSet());
   }

   public void waitForAsyncCompletion() {
      defaultBatchBackend.awaitAsyncProcessingCompletion();
   }

   public void updateIndex(Object key, Object value) {
      if (value != null) {
         if (!Thread.currentThread().isInterrupted()) {
            EntityIndexBinding entityIndexBinding = searchIntegrator.getIndexBindings().get(new PojoIndexedTypeIdentifier(value.getClass()));
            if (entityIndexBinding == null) {
               // it might be possible to receive not-indexes types
               return;
            }
            ConversionContext conversionContext = new ContextualExceptionBridgeHelper();
            DocumentBuilderIndexedEntity docBuilder = entityIndexBinding.getDocumentBuilder();
            final String idInString = keyTransformationHandler.keyToString(key);
            UpdateLuceneWork updateTask = docBuilder.createUpdateWork(
                  null,
                  docBuilder.getTypeIdentifier(),
                  value,
                  idInString,
                  idInString,
                  DefaultInstanceInitializer.DEFAULT_INITIALIZER,
                  conversionContext
            );
            try {
               defaultBatchBackend.enqueueAsyncWork(updateTask);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
      }
   }
}
