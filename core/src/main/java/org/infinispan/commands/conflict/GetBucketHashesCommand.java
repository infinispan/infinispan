package org.infinispan.commands.conflict;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.conflict.impl.BucketHash;
import org.infinispan.conflict.impl.SegmentHasher;
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
 * RPC command that requests a remote node to compute and return bucket-level hashes
 * for one or more segments. Used by the conflict manager to compare bucket hashes
 * across write owners, narrowing conflict detection to specific buckets.
 * <p>
 * Accepts multiple segments in a single request to amortize RPC latency. The response
 * is a flat list of {@link BucketHash} records; the caller groups them by
 * {@link BucketHash#segmentId()}.
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
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      InternalDataContainer<?, ?> dataContainer = registry.getInternalDataContainer().running();
      Marshaller marshaller = registry.getInternalMarshaller();
      SegmentHasher hasher = new SegmentHasher(dataContainer, marshaller);
      List<BucketHash> hashes = hasher.computeAllBucketHashes(segments, bucketCount);
      return CompletableFuture.completedFuture(hashes);
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
