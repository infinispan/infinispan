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

   public static final byte COMMAND_ID = 87;

   @ProtoField(1)
   final String cacheName;
   @ProtoField(number = 2, defaultValue = "-1")
   final int topologyId;

   @ProtoField(number = 3, defaultValue = "-1")
   final int viewId;

   final Throwable throwable;

   public RebalancePhaseConfirmCommand(String cacheName, Address origin, Throwable throwable, int topologyId, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.throwable = throwable;
      this.topologyId = topologyId;
      this.viewId = viewId;
   }

   @ProtoFactory
   RebalancePhaseConfirmCommand(String cacheName, int topologyId, int viewId, MarshallableThrowable throwable) {
      this(cacheName, null, MarshallableThrowable.unwrap(throwable), topologyId, viewId);
   }

   @ProtoField(4)
   MarshallableThrowable getThrowable() {
      return MarshallableThrowable.create(throwable);
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleRebalancePhaseConfirm(cacheName, origin, topologyId, throwable, viewId);
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
            ", viewId=" + viewId +
            '}';
   }
}
