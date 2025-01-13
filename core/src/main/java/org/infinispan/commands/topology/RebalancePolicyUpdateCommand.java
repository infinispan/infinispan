package org.infinispan.commands.topology;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Enable or Disable rebalancing.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REBALANCE_POLICY_UPDATE_COMMAND)
public class RebalancePolicyUpdateCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 88;

   @ProtoField(number = 1)
   final String cacheName;

   @ProtoField(number = 2, defaultValue = "false")
   final boolean enabled;

   @ProtoFactory
   public RebalancePolicyUpdateCommand(String cacheName, boolean enabled) {
      super(COMMAND_ID);
      this.cacheName = cacheName;
      this.enabled = enabled;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager().setRebalancingEnabled(cacheName, enabled);
   }

   @Override
   public String toString() {
      return "RebalanceEnableCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
