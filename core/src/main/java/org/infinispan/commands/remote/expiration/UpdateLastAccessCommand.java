package org.infinispan.commands.remote.expiration;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Command that will update the last access time for an entry given the specific time
 * @author wburns
 * @since 9.3
 */
public class UpdateLastAccessCommand extends BaseRpcCommand implements TopologyAffectedCommand, SegmentSpecificCommand {

   private Object key;
   private long acessTime;

   private InternalDataContainer<Object, Object> container;
   private int topologyId = -1;
   private int segment;

   public static final byte COMMAND_ID = 82;

   // Only here for CommandIdUniquenessTest
   private UpdateLastAccessCommand() { this(null); }

   public UpdateLastAccessCommand(ByteString cacheName) {
      super(cacheName);
      segment = -1;
   }

   public UpdateLastAccessCommand(ByteString cacheName, Object key, int segment, long accessTime) {
      super(cacheName);
      this.key = key;
      this.segment = segment;
      this.acessTime = accessTime;
   }

   public void inject(InternalDataContainer container) {
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
   public void writeTo(UserObjectOutput output) throws IOException {
      output.writeUserObject(key);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      UnsignedNumeric.writeUnsignedLong(output, acessTime);
   }

   @Override
   public void readFrom(UserObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readUserObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
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
      InternalCacheEntry<Object, Object> ice = container.peek(segment, key);
      if (ice != null) {
         ice.touch(acessTime);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public int getSegment() {
      return segment;
   }
}
