package org.infinispan.query.affinity;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.indexmanager.LuceneWorkConverter;
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

   private static final Log log = LogFactory.getLog(AffinityIndexManager.class, Log.class);

   private final AffinityIndexManager indexManager;
   private final RpcManager rpcManager;

   LuceneWorkDispatcher(AffinityIndexManager affinityIndexManager, RpcManager rpcManager) {
      this.indexManager = affinityIndexManager;
      this.rpcManager = rpcManager;
   }

   void dispatch(List<LuceneWork> works, ShardAddress destination, boolean originLocal) {
      if(destination.getAddress().equals(indexManager.getLocalShardAddress().getAddress())) {
         this.performLocally(works, destination.getShard(), indexManager.getKeyTransformationHandler(),
               indexManager.getSearchIntegrator(), originLocal);
      } else {
         this.sendRemotely(works, destination, originLocal);
      }
   }

   private void performLocally(Collection<LuceneWork> luceneWorks, String shard, KeyTransformationHandler handler,
                               SearchIntegrator integrator, boolean originLocal) {
      List<LuceneWork> workToApply = LuceneWorkConverter.transformKeysToString(luceneWorks, handler);
      for (LuceneWork luceneWork : workToApply) {
         AffinityIndexManager im = (AffinityIndexManager) this.getIndexManagerByName(luceneWork, shard, integrator);
         if (log.isDebugEnabled())
            log.debugf("Performing local redirected for work %s on index %s", workToApply, im.getIndexName());
         im.performOperations(Collections.singletonList(luceneWork), null, originLocal, false);
      }

   }

   private IndexManager getIndexManagerByName(LuceneWork luceneWork, String name, SearchIntegrator searchFactory) {
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

      byte[] serializedModel = indexManager.getSerializer().toSerializedModel(works);
      indexUpdateCommand.setSerializedWorkList(serializedModel);
      indexUpdateCommand.setIndexName(destination.getShard());
      Address dest = destination.getAddress();
      if (this.shouldSendSync(originLocal)) {
         log.debugf("Sending sync works %s to %s", works, dest);
         Response response = rpcManager.blocking(rpcManager.invokeCommand(dest, indexUpdateCommand,
                                                                          SingleResponseCollector.validOnly(),
                                                                          rpcManager.getSyncRpcOptions()));
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
