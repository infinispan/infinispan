package org.infinispan.query.clustered;

import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * @since 10.1
 */
@ProtoTypeId(ProtoStreamTypeIds.SEGMENTS_CLUSTERED_QUERY_COMMAND)
public class SegmentsClusteredQueryCommand extends BaseRpcCommand {

   @ProtoField(2)
   ClusteredQueryOperation clusteredQueryOperation;

   @ProtoField(3)
   BitSet segments;

   @ProtoFactory
   SegmentsClusteredQueryCommand(ByteString cacheName, ClusteredQueryOperation clusteredQueryOperation, BitSet segments) {
      super(cacheName);
      this.clusteredQueryOperation = clusteredQueryOperation;
      this.segments = segments;
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
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public CompletableFuture<?> invokeAsync(ComponentRegistry componentRegistry) {
      AdvancedCache<?, ?> cache = componentRegistry.getCache().wired();
      return perform(cache).toCompletableFuture();
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
