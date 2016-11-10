package org.infinispan.query.affinity;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.lucene.document.Document;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.indexmanager.AbstractUpdateCommand;
import org.infinispan.query.indexmanager.LuceneWorkConverter;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * Handle index updates forwarded by the {@link AffinityIndexManager}, in exceptional cases where
 * an index work ceases to be local to a node due to transient ownership changes.
 *
 * @since 9.0
 */
public class AffinityUpdateCommand extends AbstractUpdateCommand {

   private static final Log log = LogFactory.getLog(AffinityUpdateCommand.class, Log.class);

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX_AFFINITY;

   public AffinityUpdateCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public void setSerializedWorkList(byte[] serializedModel) {
      super.setSerializedWorkList(serializedModel);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      if (queryInterceptor.isStopping()) {
         throw log.cacheIsStoppingNoCommandAllowed(cacheName.toString());
      }
      List<LuceneWork> luceneWorks = searchFactory.getWorkSerializer().toLuceneWorks(serializedModel);
      KeyTransformationHandler handler = queryInterceptor.getKeyTransformationHandler();
      List<LuceneWork> workToApply = LuceneWorkConverter.transformKeysToString(luceneWorks, handler);

      for (LuceneWork luceneWork : workToApply) {
         List<IndexManager> indexManagers = getIndexManagerForModifications(luceneWork);
         try {
            for (IndexManager im : indexManagers) {
               if (log.isDebugEnabled())
                  log.debugf("Performing remote affinity work %s command on index %s", workToApply, im.getIndexName());
               AffinityIndexManager affinityIndexManager = (AffinityIndexManager) im;
               affinityIndexManager.performOperations(Collections.singletonList(luceneWork), null, false, false);
            }
         } catch (Exception e) {
            return CompletableFuture.completedFuture(new ExceptionResponse(e));
         }
      }

      return CompletableFuture.completedFuture(SuccessfulResponse.create(Boolean.TRUE));
   }

   private List<IndexManager> getIndexManagerForModifications(LuceneWork luceneWork) {
      Class<?> entityClass = luceneWork.getEntityClass();
      Serializable id = luceneWork.getId();
      if (id != null) {
         String idInString = luceneWork.getIdInString();
         Document document = luceneWork.getDocument();
         return Arrays.asList(searchFactory.getIndexBinding(entityClass).getSelectionStrategy()
               .getIndexManagerForAddition(entityClass, id, idInString, document));
      } else {
         return Arrays.asList(searchFactory.getIndexBinding(entityClass)
               .getSelectionStrategy().getIndexManagersForDeletion(entityClass, null, null));
      }
   }

   @Override
   public byte getCommandId() {
      return ModuleCommandIds.UPDATE_INDEX_AFFINITY;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

}
