package org.infinispan.server.iteration;

import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.container.entries.CacheEntry;

/**
 * @author wburns
 * @since 9.0
 */
public class IterableIterationResult {

   public enum Status {
      Success,
      Finished,
      InvalidIteration,
   }

   private final Set<Integer> finishedSegments;
   private final Status statusCode;
   private final List<CacheEntry> entries;
   private final boolean metadata;
   private final Function<Object, Object> resultFunction;

   IterableIterationResult(Set<Integer> finishedSegments, Status statusCode, List<CacheEntry> entries, boolean metadata, Function<Object, Object> resultFunction) {
      this.finishedSegments = finishedSegments;
      this.statusCode = statusCode;
      this.entries = entries;
      this.metadata = metadata;
      this.resultFunction = resultFunction;
   }

   public Status getStatusCode() {
      return statusCode;
   }

   public List<CacheEntry> getEntries() {
      return entries;
   }

   public boolean isMetadata() {
      return metadata;
   }

   public byte[] segmentsToBytes() {
      BitSet bs = new BitSet();
      finishedSegments.stream().forEach(bs::set);
      return bs.toByteArray();
   }

   @Override
   public String toString() {
      return "IterableIterationResult{" +
            "finishedSegments=" + finishedSegments +
            ", statusCode=" + statusCode +
            ", entries=" + entries +
            ", metadata=" + metadata +
            '}';
   }

   public Function<Object, Object> getResultFunction() {
      return resultFunction;
   }
}
