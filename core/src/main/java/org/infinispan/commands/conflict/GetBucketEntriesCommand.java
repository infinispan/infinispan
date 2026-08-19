package org.infinispan.commands.conflict;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.conflict.impl.SegmentHasher;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;

import io.reactivex.rxjava3.core.Flowable;

/**
 * RPC command that requests entries from specific buckets within a segment.
 * Used by the conflict manager to fetch only the entries from mismatched buckets
 * rather than all entries in the segment.
 * <p>
 * Uses {@link LocalPublisherManager} so that non-shared stores are included in the
 * results alongside in-memory entries.
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
   @SuppressWarnings("unchecked")
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      LocalPublisherManager<Object, Object> lpm =
            (LocalPublisherManager<Object, Object>) registry.getLocalPublisherManager().running();
      boolean allBuckets = bucketIds.size() >= bucketCount;
      Marshaller marshaller = allBuckets ? null : registry.getInternalMarshaller();

      long flags = EnumUtil.bitSetOf(Flag.STATE_TRANSFER_PROGRESS);
      return Flowable.fromPublisher(
                  lpm.entryPublisher(
                        IntSets.immutableSet(segmentId), null, null,
                        flags, DeliveryGuarantee.AT_MOST_ONCE, Function.identity())
                     .publisherWithoutSegments())
            .filter(entry -> allBuckets || bucketIds.contains(SegmentHasher.computeBucket(entry.getKey(), bucketCount, marshaller)))
            .collect(ArrayList::new, List::add)
            .toCompletionStage();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
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
