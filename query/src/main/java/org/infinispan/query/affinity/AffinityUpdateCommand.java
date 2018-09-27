package org.infinispan.query.affinity;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.apache.lucene.document.Document;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.indexmanager.AbstractUpdateCommand;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * Handle index updates forwarded by the {@link AffinityIndexManager}, in exceptional cases where
 * an index work ceases to be local to a node due to transient ownership changes.
 * <p>
 * This class is public so it can be used by other internal Infinispan packages but should not be considered part of a
 * public API.
 *
 * @since 9.0
 */
public final class AffinityUpdateCommand extends AbstractUpdateCommand {

   private static final Log log = LogFactory.getLog(AffinityUpdateCommand.class, Log.class);

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX_AFFINITY;

   public AffinityUpdateCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      if (queryInterceptor.isStopping()) {
         throw log.cacheIsStoppingNoCommandAllowed(cacheName.toString());
      }
      for (LuceneWork luceneWork : getLuceneWorks()) {
         Iterable<IndexManager> indexManagers = getIndexManagerForModifications(luceneWork);
         try {
            for (IndexManager im : indexManagers) {
               if (log.isDebugEnabled())
                  log.debugf("Performing remote affinity work %s command on index %s", luceneWork, im.getIndexName());
               AffinityIndexManager affinityIndexManager = (AffinityIndexManager) im;
               affinityIndexManager.performOperations(Collections.singletonList(luceneWork), null, false, false);
            }
         } catch (Exception e) {
            return CompletableFuture.completedFuture(new ExceptionResponse(e));
         }
      }

      return CompletableFuture.completedFuture(Boolean.TRUE);
   }

   private Iterable<IndexManager> getIndexManagerForModifications(LuceneWork luceneWork) {
      IndexedTypeIdentifier type = luceneWork.getEntityType();
      Serializable id = luceneWork.getId();
      if (id != null) {
         String idInString = luceneWork.getIdInString();
         Document document = luceneWork.getDocument();
         return Collections.singleton(searchFactory.getIndexBinding(type)
               .getIndexManagerSelector()
               .forNew(type, id, idInString, document));
      } else {
         return searchFactory.getIndexBinding(type)
               .getIndexManagerSelector()
               .forExisting(type, null, null);
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
