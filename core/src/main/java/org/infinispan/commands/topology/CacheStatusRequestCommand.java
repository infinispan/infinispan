package org.infinispan.commands.topology;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The coordinator is requesting information about the running caches.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_STATUS_REQUEST_COMMAND)
public class CacheStatusRequestCommand extends AbstractCacheControlCommand {

   private final static Log log = LogFactory.getLog(CacheStatusRequestCommand.class);

   @ProtoField(1)
   final int viewId;

   @ProtoFactory
   public CacheStatusRequestCommand(int viewId) {
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      if (!gcr.isLocalTopologyManagerRunning()) {
         log.debug("Reply with empty status request because topology manager not running");
         return CompletableFuture.completedFuture(new ManagerStatusResponse(Collections.emptyMap(), true));
      }

      return gcr.getLocalTopologyManager()
            .handleStatusRequest(viewId);
   }

   @Override
   public String toString() {
      return "CacheStatusRequestCommand{" +
            "viewId=" + viewId +
            '}';
   }
}
