package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;

/**
 * A node is requesting to join the cluster.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class CacheJoinCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 85;

   private String cacheName;
   private CacheJoinInfo joinInfo;
   private int viewId;

   // For CommandIdUniquenessTest only
   public CacheJoinCommand() {
      super(COMMAND_ID);
   }

   public CacheJoinCommand(String cacheName, Address origin, CacheJoinInfo joinInfo, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.joinInfo = joinInfo;
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      return gcr.getClusterTopologyManager()
            .handleJoin(cacheName, origin, joinInfo, viewId);
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      output.writeObject(joinInfo);
      output.writeInt(viewId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      joinInfo = (CacheJoinInfo) input.readObject();
      viewId = input.readInt();
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
