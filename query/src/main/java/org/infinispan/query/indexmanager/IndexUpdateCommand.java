package org.infinispan.query.indexmanager;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.exception.SearchException;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.logging.Log;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Custom RPC command containing an index update request for the Master IndexManager of a specific cache and index.
 * <p>
 * This class is public so it can be used by other internal Infinispan packages but should not be considered part of a
 * public API.
 *
 * @author Sanne Grinovero
 */
public final class IndexUpdateCommand extends AbstractUpdateCommand {

   private static final Log log = LogFactory.getLog(IndexUpdateCommand.class, Log.class);

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX;

   public IndexUpdateCommand(ByteString cacheName) {
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
      //idInString field is not serialized, we need to extract it from the key object
      indexManager.performOperations(getLuceneWorks(), null);
      return CompletableFutures.completedNull(); //Return value to be ignored
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }
}
