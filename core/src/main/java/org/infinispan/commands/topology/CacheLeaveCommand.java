package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;

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

   @ProtoField(2)
   final long timeout;

   @ProtoFactory
   CacheLeaveCommand(String cacheName, long timeout) {
      this(cacheName, null, timeout);
   }

   public CacheLeaveCommand(String cacheName, Address origin, long timeout) {
      super(origin);
      this.cacheName = cacheName;
      this.timeout = timeout;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleLeave(cacheName, origin, timeout, TimeUnit.MILLISECONDS);
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "CacheLeaveCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            '}';
   }
}
