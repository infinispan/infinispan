package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;

/**
 * A node is requesting to join the cluster.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_JOIN_COMMAND)
public class CacheJoinCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 85;

   @ProtoField(number = 1)
   final String cacheName;

   @ProtoField(number = 2)
   final CacheJoinInfo joinInfo;

   @ProtoField(number = 3, defaultValue = "-1")
   final int viewId;

   @ProtoFactory
   CacheJoinCommand(String cacheName, CacheJoinInfo joinInfo, int viewId) {
      this(cacheName, null, joinInfo, viewId);
   }

   public CacheJoinCommand(String cacheName, Address origin, CacheJoinInfo joinInfo, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.joinInfo = joinInfo;
      this.viewId = viewId;
   }

   public String getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleJoin(cacheName, origin, joinInfo, viewId);
   }

   @Override
   public String toString() {
      return "TopologyJoinCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", joinInfo=" + joinInfo +
            ", viewId=" + viewId +
            '}';
   }
}
