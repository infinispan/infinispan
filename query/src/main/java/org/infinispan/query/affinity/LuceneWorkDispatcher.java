package org.infinispan.query.affinity;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.serialization.spi.LuceneWorkSerializer;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.query.logging.Log;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * Dispatches {@link LuceneWork} locally (to other shard in the same node) or remotely.
 *
 * @since 9.0
 */
class LuceneWorkDispatcher {

   private static final Log log = LogFactory.getLog(LuceneWorkDispatcher.class, Log.class);

   private final AffinityIndexManager indexManager;
   private final RpcManager rpcManager;
   private final LuceneWorkSerializer luceneWorkSerializer;

   LuceneWorkDispatcher(AffinityIndexManager affinityIndexManager, RpcManager rpcManager) {
      this.indexManager = affinityIndexManager;
      this.rpcManager = rpcManager;
      this.luceneWorkSerializer = indexManager.getSearchIntegrator().getWorkSerializer();
   }

   void dispatch(List<LuceneWork> works, ShardAddress destination, boolean originLocal) {
      if (destination.getAddress().equals(indexManager.getLocalShardAddress().getAddress())) {
         performLocally(works, destination.getShard(), indexManager.getSearchIntegrator(), originLocal);
      } else {
         sendRemotely(works, destination, originLocal);
      }
   }

   private void performLocally(Collection<LuceneWork> luceneWorks, String shard, SearchIntegrator integrator, boolean originLocal) {
      for (LuceneWork luceneWork : luceneWorks) {
         AffinityIndexManager im = (AffinityIndexManager) getIndexManagerByName(luceneWork, shard, integrator);
         if (log.isDebugEnabled())
            log.debugf("Performing local redirected for work %s on index %s", luceneWork, im.getIndexName());
         im.performOperations(Collections.singletonList(luceneWork), null, originLocal, false);
      }
   }

   private IndexManager getIndexManagerByName(LuceneWork luceneWork, String name, SearchIntegrator searchFactory) {
//      return searchIntegrator.getIndexBinding(luceneWork.getEntityType())
//            .getIndexManagerSelector().all()
//            .stream().findFirst().filter(im -> im.getIndexName().equals(name))
//            .get();
      IndexedTypeIdentifier entityClass = luceneWork.getEntityType();
      Set<IndexManager> indexManagersForAllShards =
            searchFactory.getIndexBinding(entityClass).getIndexManagerSelector().all();
      return indexManagersForAllShards.stream().filter(im -> im.getIndexName().equals(name)).iterator().next();
   }


   private boolean shouldSendSync(boolean originLocal) {
      return !indexManager.isAsync() && originLocal;
   }

   private void sendRemotely(List<LuceneWork> works, ShardAddress destination, boolean originLocal) {
      String cacheName = indexManager.getCacheName();
      AffinityUpdateCommand indexUpdateCommand = new AffinityUpdateCommand(ByteString.fromString(cacheName));
      indexUpdateCommand.setSerializedLuceneWorks(luceneWorkSerializer.toSerializedModel(works));
      indexUpdateCommand.setIndexName(destination.getShard());
      Address dest = destination.getAddress();
      if (this.shouldSendSync(originLocal)) {
         log.debugf("Sending sync works %s to %s", works, dest);
         Response response = rpcManager.blocking(rpcManager.invokeCommand(dest, indexUpdateCommand,
               SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions()));
         log.debugf("Response %s obtained for command %s", response, works);
      } else {
         log.debugf("Sending async works %s to %s", works, dest);
         CompletionStage<ValidResponse> result = rpcManager.invokeCommand(
               dest, indexUpdateCommand, SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions());
         result.whenComplete((responses, error) -> {
            if (error != null) {
               log.error("Error forwarding index job", error);
            }
            log.debugf("Async work %s applied successfully with response %s", works, responses);
         });
      }
   }
}
