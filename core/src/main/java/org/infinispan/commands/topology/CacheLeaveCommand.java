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

   public static final byte COMMAND_ID = 86;

   @ProtoField(number = 1)
   final String cacheName;

   @ProtoField(number = 2, defaultValue = "-1")
   final int viewId;

   @ProtoFactory
   CacheLeaveCommand(String cacheName, int viewId) {
      this(cacheName, null, viewId);
   }

   public CacheLeaveCommand(String cacheName, Address origin, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleLeave(cacheName, origin, viewId);
   }

   @Override
   public String toString() {
      return "TopologyLeaveCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", viewId=" + viewId +
            '}';
   }
}
