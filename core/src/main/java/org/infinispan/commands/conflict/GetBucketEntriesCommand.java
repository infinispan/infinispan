package org.infinispan.commands.conflict;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.conflict.impl.SegmentHasher;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;

/**
 * RPC command that requests entries from specific buckets within a segment.
 * Used by the conflict manager to fetch only the entries from mismatched buckets
 * rather than all entries in the segment.
 */
@ProtoTypeId(ProtoStreamTypeIds.GET_BUCKET_ENTRIES_COMMAND)
public class GetBucketEntriesCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private int topologyId;
   private final int segmentId;
   private final IntSet bucketIds;
   private final int bucketCount;

   @ProtoFactory
   GetBucketEntriesCommand(ByteString cacheName, int topologyId, int segmentId,
                           WrappedMessage wrappedBucketIds, int bucketCount) {
      this(cacheName, topologyId, segmentId, WrappedMessages.<IntSet>unwrap(wrappedBucketIds), bucketCount);
   }

   public GetBucketEntriesCommand(ByteString cacheName, int topologyId, int segmentId,
                                  IntSet bucketIds, int bucketCount) {
      super(cacheName);
      this.topologyId = topologyId;
      this.segmentId = segmentId;
      this.bucketIds = bucketIds;
      this.bucketCount = bucketCount;
   }

   @Override
   @ProtoField(2)
   public int getTopologyId() {
      return topologyId;
   }

   @ProtoField(3)
   public int getSegmentId() {
      return segmentId;
   }

   @ProtoField(4)
   WrappedMessage getWrappedBucketIds() {
      return WrappedMessages.orElseNull(bucketIds);
   }

   @ProtoField(5)
   public int getBucketCount() {
      return bucketCount;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      InternalDataContainer<?, ?> dataContainer = registry.getInternalDataContainer().running();
      boolean allBuckets = bucketIds.size() >= bucketCount;
      SegmentHasher hasher = allBuckets ? null
            : new SegmentHasher(dataContainer, registry.getInternalMarshaller());

      List<CacheEntry<?, ?>> result = new ArrayList<>();
      Iterator<InternalCacheEntry<?, ?>> it = cast(dataContainer.iterator(IntSets.immutableSet(segmentId)));
      while (it.hasNext()) {
         InternalCacheEntry<?, ?> entry = it.next();
         if (allBuckets || bucketIds.contains(hasher.bucketForKey(entry.getKey(), bucketCount))) {
            result.add(entry);
         }
      }
      return CompletableFuture.completedFuture(result);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @SuppressWarnings("unchecked")
   private static Iterator<InternalCacheEntry<?, ?>> cast(Iterator<? extends InternalCacheEntry<?, ?>> it) {
      return (Iterator<InternalCacheEntry<?, ?>>) (Iterator<?>) it;
   }

   @Override
   public String toString() {
      return "GetBucketEntriesCommand{" +
            "topologyId=" + topologyId +
            ", segmentId=" + segmentId +
            ", bucketIds=" + bucketIds +
            ", bucketCount=" + bucketCount +
            ", cacheName=" + cacheName +
            '}';
   }
}
