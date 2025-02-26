package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableThrowable;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A member is confirming that it has finished a topology change during rebalance.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REBALANCE_PHASE_CONFIRM_COMMAND)
public class RebalancePhaseConfirmCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;
   @ProtoField(2)
   final int topologyId;

   final Throwable throwable;

   public RebalancePhaseConfirmCommand(String cacheName, Address origin, Throwable throwable, int topologyId) {
      super(origin);
      this.cacheName = cacheName;
      this.throwable = throwable;
      this.topologyId = topologyId;
   }

   @ProtoFactory
   RebalancePhaseConfirmCommand(String cacheName, int topologyId, MarshallableThrowable throwable) {
      this(cacheName, null, MarshallableThrowable.unwrap(throwable), topologyId);
   }

   @ProtoField(3)
   MarshallableThrowable getThrowable() {
      return MarshallableThrowable.create(throwable);
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleRebalancePhaseConfirm(cacheName, origin, topologyId, throwable);
   }

   public String getCacheName() {
      return cacheName;
   }

   @Override
   public String toString() {
      return "RebalancePhaseConfirmCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", throwable=" + throwable +
            ", topologyId=" + topologyId +
            '}';
   }
}
