package org.infinispan.commands.remote.expiration;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Command that will update the last access time for an entry given the specific time
 * @author wburns
 * @since 9.3
 */
public class UpdateLastAccessCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private Object key;
   private long acessTime;

   private DataContainer<Object, Object> container;
   private int topologyId = -1;

   public static final byte COMMAND_ID = 82;

   // Only here for CommandIdUniquenessTest
   private UpdateLastAccessCommand() { super(null); }

   public UpdateLastAccessCommand(ByteString cacheName) {
      super(cacheName);
   }

   public UpdateLastAccessCommand(ByteString cacheName, Object key, long accessTime) {
      super(cacheName);
      this.key = key;
      this.acessTime = accessTime;
   }

   public void inject(DataContainer container) {
      this.container = container;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      UnsignedNumeric.writeUnsignedLong(output, acessTime);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      acessTime = UnsignedNumeric.readUnsignedLong(input);
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      InternalCacheEntry<Object, Object> ice = container.peek(key);
      if (ice != null) {
         ice.touch(acessTime);
      }
      return CompletableFutures.completedNull();
   }
}
