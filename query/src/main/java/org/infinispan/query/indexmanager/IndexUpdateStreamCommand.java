package org.infinispan.query.indexmanager;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.logging.Log;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Execute a stream operation.
 * <p>
 * This class is public so it can be used by other internal Infinispan packages but should not be considered part of a
 * public API.
 *
 * @author gustavonalle
 * @since 7.0
 */
public final class IndexUpdateStreamCommand extends AbstractUpdateCommand {

   private static final Log log = LogFactory.getLog(IndexUpdateStreamCommand.class, Log.class);

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX_STREAM;

   public IndexUpdateStreamCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      if (queryInterceptor.isStopping()) {
         throw log.cacheIsStoppingNoCommandAllowed(cacheName.toString());
      }
      IndexManager indexManager = searchFactory.getIndexManager(indexName);
      if (indexManager == null) {
         throw new SearchException("Unknown index referenced : " + indexName);
      }
      LuceneWork workToApply = getLuceneWorks().get(0);
      indexManager.performStreamOperation(workToApply, null, true);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }
}
