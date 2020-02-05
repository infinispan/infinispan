package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * Enable or Disable rebalancing.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class RebalancePolicyUpdateCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 88;

   private String cacheName;
   private boolean enabled;

   // For CommandIdUniquenessTest only
   public RebalancePolicyUpdateCommand() {
      super(COMMAND_ID);
   }

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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      output.writeBoolean(enabled);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      enabled = input.readBoolean();
   }

   @Override
   public String toString() {
      return "RebalanceEnableCommand{" +
            "cacheName='" + cacheName + '\'' +
            '}';
   }
}
