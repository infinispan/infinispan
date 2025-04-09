package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Tell members to shutdown cache.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_SHUTDOWN_COMMAND)
public class CacheShutdownCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;

   @ProtoFactory
   public CacheShutdownCommand(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getLocalTopologyManager()
            .handleCacheShutdown(cacheName);
   }

   @Override
   public String toString() {
      return "ShutdownCacheCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
