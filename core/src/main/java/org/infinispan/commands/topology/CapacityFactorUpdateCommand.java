package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;

/**
 * Updates the capacity factor for a node in a cache.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CAPACITY_FACTOR_UPDATE_COMMAND)
public class CapacityFactorUpdateCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;

   @ProtoField(2)
   final float capacityFactor;

   public CapacityFactorUpdateCommand(Address origin, String cacheName, float capacityFactor) {
      super(origin);
      this.cacheName = cacheName;
      this.capacityFactor = capacityFactor;
   }

   @ProtoFactory
   CapacityFactorUpdateCommand(String cacheName, float capacityFactor) {
      this(null, cacheName, capacityFactor);
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .setCapacityFactor(cacheName, origin, capacityFactor);
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN_TWO;
   }

   @Override
   public String toString() {
      return "CapacityFactorUpdateCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", capacityFactor=" + capacityFactor +
            ", origin=" + origin +
            '}';
   }
}
