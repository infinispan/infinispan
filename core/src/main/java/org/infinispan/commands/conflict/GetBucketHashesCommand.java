package org.infinispan.commands.conflict;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.conflict.impl.BucketHash;
import org.infinispan.conflict.impl.SegmentHashTracker;
import org.infinispan.conflict.impl.SegmentHasher;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
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
 * RPC command that requests a remote node to compute and return bucket-level hashes
 * for one or more segments. Used by the conflict manager to compare bucket hashes
 * across write owners, narrowing conflict detection to specific buckets.
 * <p>
 * Accepts multiple segments in a single request to amortize RPC latency. The response
 * is a flat list of {@link BucketHash} records; the caller groups them by
 * {@link BucketHash#segmentId()}.
 * <p>
 * When the {@link SegmentHashTracker} is enabled its pre-computed hashes are returned
 * directly. Otherwise hashes are computed on-the-fly using {@link LocalPublisherManager}
 * so that non-shared stores are included alongside in-memory entries.
 */
@ProtoTypeId(ProtoStreamTypeIds.GET_BUCKET_HASHES_COMMAND)
public class GetBucketHashesCommand extends BaseRpcCommand implements TopologyAffectedCommand {

   private int topologyId;
   private final IntSet segments;
   private final int bucketCount;

   @ProtoFactory
   GetBucketHashesCommand(ByteString cacheName, int topologyId, WrappedMessage wrappedSegments, int bucketCount) {
      this(cacheName, topologyId, WrappedMessages.<IntSet>unwrap(wrappedSegments), bucketCount);
   }

   public GetBucketHashesCommand(ByteString cacheName, int topologyId, IntSet segments, int bucketCount) {
      super(cacheName);
      this.topologyId = topologyId;
      this.segments = segments;
      this.bucketCount = bucketCount;
   }

   @Override
   @ProtoField(2)
   public int getTopologyId() {
      return topologyId;
   }

   @ProtoField(3)
   WrappedMessage getWrappedSegments() {
      return WrappedMessages.orElseNull(segments);
   }

   @ProtoField(4)
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
      SegmentHashTracker tracker = registry.getComponent(SegmentHashTracker.class);
      if (tracker != null && tracker.isEnabled()) {
         return CompletableFuture.completedFuture(tracker.getAllBucketHashes(segments));
      }
      // Tracker not available: compute hashes on-the-fly via LocalPublisherManager so
      // that non-shared store entries are included in the hash alongside in-memory entries.
      Marshaller marshaller = registry.getInternalMarshaller();
      KeyPartitioner keyPartitioner = registry.getComponent(KeyPartitioner.class);
      LocalPublisherManager<Object, Object> lpm =
            (LocalPublisherManager<Object, Object>) registry.getLocalPublisherManager().running();
      long flags = EnumUtil.bitSetOf(Flag.STATE_TRANSFER_PROGRESS);
      int numSegments = registry.getDistributionManager().getCacheTopology()
            .getWriteConsistentHash().getNumSegments();
      long[] hashes = new long[numSegments * bucketCount];
      int[] counts = new int[numSegments * bucketCount];

      return Flowable.fromPublisher(
                  lpm.entryPublisher(segments, null, null,
                        flags, DeliveryGuarantee.AT_MOST_ONCE, Function.identity())
                     .publisherWithoutSegments())
            .doOnNext(entry -> {
               int seg = keyPartitioner.getSegment(entry.getKey());
               long keyHash = SegmentHasher.hashObject(entry.getKey(), marshaller);
               int bucket = (int) (keyHash & (bucketCount - 1));
               int idx = seg * bucketCount + bucket;
               hashes[idx] ^= keyHash ^ SegmentHasher.hashObject(entry.getValue(), marshaller);
               counts[idx]++;
            })
            .ignoreElements()
            .toCompletionStage(null)
            .thenApply(ignored -> {
               List<BucketHash> result = new ArrayList<>(segments.size() * bucketCount);
               segments.forEach((int seg) -> {
                  for (int b = 0; b < bucketCount; b++) {
                     int idx = seg * bucketCount + b;
                     result.add(new BucketHash(seg, b, hashes[idx], counts[idx]));
                  }
               });
               return result;
            });
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
      return "GetBucketHashesCommand{" +
            "topologyId=" + topologyId +
            ", segments=" + segments +
            ", bucketCount=" + bucketCount +
            ", cacheName=" + cacheName +
            '}';
   }
}
