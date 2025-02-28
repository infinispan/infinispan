package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A member is requesting a cache shutdown.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_SHUTDOWN_REQUEST_COMMAND)
public class CacheShutdownRequestCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;

   @ProtoFactory
   public CacheShutdownRequestCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleShutdownRequest(cacheName);
   }

   @Override
   public String toString() {
      return "ShutdownCacheRequestCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
