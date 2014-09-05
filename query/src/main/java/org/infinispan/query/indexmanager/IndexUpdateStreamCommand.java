package org.infinispan.query.indexmanager;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.impl.ModuleCommandIds;

import java.util.List;

/**
 * Execute a stream operation
 *
 * @author gustavonalle
 * @since 7.0
 */
public class IndexUpdateStreamCommand extends AbstractUpdateCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX_STREAM;

   public IndexUpdateStreamCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      if (queryInterceptor.isStopping()) {
         throw log.cacheIsStoppingNoCommandAllowed(cacheName);
      }
      IndexManager indexManager = searchFactory.getIndexManagerHolder().getIndexManager(indexName);
      if (indexManager == null) {
         throw new SearchException("Unknown index referenced : " + indexName);
      }
      List<LuceneWork> luceneWorks = indexManager.getSerializer().toLuceneWorks(this.serializedModel);
      LuceneWork workToApply = transformKeyToStrings(luceneWorks.iterator().next());
      indexManager.performStreamOperation(workToApply, null, true);
      return Boolean.TRUE;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

}
