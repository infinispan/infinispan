package org.infinispan.query.affinity;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.lucene.document.Document;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.indexmanager.AbstractUpdateCommand;
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
      List<LuceneWork> workToApply = transformKeysToStrings(luceneWorks);

      for (LuceneWork luceneWork : luceneWorks) {
         IndexManager im = getIndexManagerForWrites(luceneWork);
         try {
            if (log.isDebugEnabled())
               log.debugf("Performing remote affinity work %s command on index %s", workToApply, im.getIndexName());
            im.performOperations(Collections.singletonList(luceneWork), null);
         } catch (Exception e) {
            return CompletableFuture.completedFuture(new ExceptionResponse(e));
         }
      }

      return CompletableFuture.completedFuture(SuccessfulResponse.create(Boolean.TRUE));
   }

   private IndexManager getIndexManagerForWrites(LuceneWork luceneWork) {
      Class<?> entityClass = luceneWork.getEntityClass();
      Serializable id = luceneWork.getId();
      String idInString = luceneWork.getIdInString();
      Document document = luceneWork.getDocument();
      return searchFactory.getIndexBinding(entityClass).getSelectionStrategy()
              .getIndexManagerForAddition(entityClass, id, idInString, document);
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
