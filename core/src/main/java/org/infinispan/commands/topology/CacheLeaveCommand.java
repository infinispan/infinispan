package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.Address;

/**
 * A node is signaling that it wants to leave the cluster.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class CacheLeaveCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 86;

   private String cacheName;
   private int viewId;

   // For CommandIdUniquenessTest only
   public CacheLeaveCommand() {
      super(COMMAND_ID);
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
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      output.writeInt(viewId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      viewId = input.readInt();
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
