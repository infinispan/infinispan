package org.infinispan.commands.remote.expiration;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.util.ByteString;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Command that when invoked will retrieve the last access time from an entry without updating it
 * @author wburns
 * @since 9.3
 */
public class RetrieveLastAccessCommand extends BaseRpcCommand implements TopologyAffectedCommand, SegmentSpecificCommand {

   private Object key;
   private Object value;

   private InternalDataContainer<Object, Object> container;
   private TimeService timeService;
   private int topologyId = -1;
   private int segment;

   public static final byte COMMAND_ID = 81;

   // Only here for CommandIdUniquenessTest
   private RetrieveLastAccessCommand() {
      this(null);

   }

   public RetrieveLastAccessCommand(ByteString cacheName) {
      super(cacheName);
      segment = -1;
   }

   public RetrieveLastAccessCommand(ByteString cacheName, Object key, Object value, int segment) {
      super(cacheName);
      this.key = Objects.requireNonNull(key);
      this.value = value;
      this.segment = segment;
   }

   public void inject(InternalDataContainer container, TimeService timeService) {
      this.container = container;
      this.timeService = timeService;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      if (value == null) {
         output.writeBoolean(false);
      } else {
         output.writeBoolean(true);
         output.writeObject(value);
      }
      UnsignedNumeric.writeUnsignedInt(output, segment);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      boolean hasValue = input.readBoolean();
      if (hasValue) {
         value = input.readObject();
      }
      segment = UnsignedNumeric.readUnsignedInt(input);
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
      if (ice != null && (value == null || value.equals(ice.getValue())) &&
            !ice.isExpired(timeService.wallClockTime())) {
         return CompletableFuture.completedFuture(ice.getLastUsed());
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public int getSegment() {
      return segment;
   }
}
