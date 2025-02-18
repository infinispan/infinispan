package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.Address;

/**
 * A member is confirming that it has finished a topology change during rebalance.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class RebalancePhaseConfirmCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 87;

   private String cacheName;
   private Throwable throwable;
   private int topologyId;

   // For CommandIdUniquenessTest only
   public RebalancePhaseConfirmCommand() {
      super(COMMAND_ID);
   }

   public RebalancePhaseConfirmCommand(String cacheName, Address origin, Throwable throwable, int topologyId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.throwable = throwable;
      this.topologyId = topologyId;
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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      output.writeObject(throwable);
      output.writeInt(topologyId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      throwable = (Throwable) input.readObject();
      topologyId = input.readInt();
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
