package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A node is signaling that it wants to leave the cluster.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_LEAVE_COMMAND)
public class CacheLeaveCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;

   @ProtoFactory
   CacheLeaveCommand(String cacheName) {
      this(cacheName, null);
   }

   public CacheLeaveCommand(String cacheName, Address origin) {
      super(origin);
      this.cacheName = cacheName;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleLeave(cacheName, origin);
   }

   @Override
   public String toString() {
      return "CacheLeaveCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            '}';
   }
}
