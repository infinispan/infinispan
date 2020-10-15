package org.infinispan.persistence.async;

import java.util.concurrent.CompletionStage;

import org.infinispan.persistence.spi.MarshallableEntry;

interface Modification {
   /**
    * Applies the modification to the provided async store.
    * <p>
    * This method is not thread safe, callers must ensure that it is not invoked on multiple threads in parallel.
    * @param store the store to apply the modification to
    * @param <K> key type
    * @param <V> value type
    */
   <K, V> void apply(AsyncNonBlockingStore<K, V> store);

   /**
    * Returns the segment that maps to this modification. Some modifications may not map to a given
    * segment and may throw an {@link UnsupportedOperationException}.
    * @return the segment that maps to the modification
    */
   int getSegment();

   /**
    * Returns this modification as a stage that is already complete.
    * @param <K> key type
    * @param <V> value type
    * @return a stage that represents the modification
    */
   <K, V> CompletionStage<MarshallableEntry<K, V>> asStage();
}
