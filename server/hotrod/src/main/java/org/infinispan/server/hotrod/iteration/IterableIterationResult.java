package org.infinispan.server.hotrod.iteration;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.OperationStatus;

/**
 * @author wburns
 * @since 9.0
 */
public class IterableIterationResult {
   private final Set<Integer> finishedSegments;
   private final OperationStatus statusCode;
   private final List<CacheEntry> entries;
   private final CompatInfo compatInfo;
   private final boolean metadata;

   IterableIterationResult(Set<Integer> finishedSegments, OperationStatus statusCode, List<CacheEntry> entries, CompatInfo compatInfo, boolean metadata) {
      this.finishedSegments = finishedSegments;
      this.statusCode = statusCode;
      this.entries = entries;
      this.compatInfo = compatInfo;
      this.metadata = metadata;
   }

   public OperationStatus getStatusCode() {
      return statusCode;
   }

   public List<CacheEntry> getEntries() {
      return entries;
   }

   public boolean isMetadata() {
      return metadata;
   }

   public boolean isCompatEnabled() {
      return compatInfo.enabled;
   }

   public byte[] segmentsToBytes() {
      BitSet bs = new BitSet();
      finishedSegments.stream().forEach(bs::set);
      return bs.toByteArray();
   }

   public Object unbox(Object value) {
      if(value == null) return null;
      return compatInfo.valueEncoder.fromStorage(value);
   }

   @Override
   public String toString() {
      return "IterableIterationResult{" +
            "finishedSegments=" + finishedSegments +
            ", statusCode=" + statusCode +
            ", entries=" + entries +
            ", compatInfo=" + compatInfo +
            ", metadata=" + metadata +
            '}';
   }
}
