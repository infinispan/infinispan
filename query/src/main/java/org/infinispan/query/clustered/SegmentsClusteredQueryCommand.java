package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.util.ByteString;

/**
 * @since 10.1
 */
public class SegmentsClusteredQueryCommand extends BaseRpcCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.SEGMENT_CLUSTERED_QUERY;

   private ClusteredQueryOperation clusteredQueryOperation;
   private BitSet segments;

   public SegmentsClusteredQueryCommand(ByteString cacheName) {
      super(cacheName);
   }

   public SegmentsClusteredQueryCommand(String cacheName, ClusteredQueryOperation clusteredQueryOperation, BitSet segments) {
      super(ByteString.fromString(cacheName));
      this.clusteredQueryOperation = clusteredQueryOperation;
      this.segments = segments;
   }

   public void setSegments(BitSet segments) {
      this.segments = segments;
   }

   public BitSet getSegments() {
      return segments;
   }

   public CompletionStage<QueryResponse> perform(Cache<?, ?> cache) {
      return clusteredQueryOperation.perform(cache, segments);
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
      output.writeObject(clusteredQueryOperation);
      byte[] bytes = segments.toByteArray();
      int length = bytes.length;
      output.write(length);
      if (length > 0) {
         output.write(bytes);
      }
   }

   @Override
   public CompletableFuture<?> invokeAsync(ComponentRegistry componentRegistry) {
      AdvancedCache<?, ?> cache = componentRegistry.getCache().wired();
      return perform(cache).toCompletableFuture();
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      this.clusteredQueryOperation = (ClusteredQueryOperation) input.readObject();
      int len = input.readUnsignedByte();
      BitSet bitSet = null;
      if (len > 0) {
         byte[] b = new byte[len];
         input.readFully(b);
         bitSet = BitSet.valueOf(b);
      }
      this.segments = bitSet;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

}
