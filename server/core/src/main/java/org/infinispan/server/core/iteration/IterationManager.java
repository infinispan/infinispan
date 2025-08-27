package org.infinispan.server.core.iteration;

import java.util.BitSet;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;

/**
 * @author wburns
 * @since 9.0
 */
public interface IterationManager {
   IterationState start(AdvancedCache cache, BitSet segments, String filterConverterFactory,
                        List<byte[]> filterConverterParams, MediaType valueMediaType, int batch, boolean metadata,
                        DeliveryGuarantee guarantee, IterationInitializationContext ctx);

   /**
    * Reds the next batch from the iterator associated with {@param iterationId}.
    *
    * <p>
    * <b>Warning:</b> This method can block.
    * </p>
    *
    * @param iterationId: The iterator identifier.
    * @param batch: The maximum number of entries to include in the batch.
    * @return An {@link IterableIterationResult} object with the current data and metadata about the iterator.
    */
   IterableIterationResult next(String iterationId, int batch);

   IterationState close(String iterationId);

   void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory);

   void removeKeyValueFilterConverterFactory(String name);

   int activeIterations();
}
