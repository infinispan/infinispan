package org.infinispan.server.iteration;

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
   IterationState start(AdvancedCache cache, BitSet segments,
                        String filterConverterFactory, List<byte[]> filterConverterParams, MediaType valueMediaType,
                        int batch, boolean metadata, DeliveryGuarantee guarantee);

   IterableIterationResult next(String iterationId, int batch);

   IterationState close(String iterationId);

   void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory);

   void removeKeyValueFilterConverterFactory(String name);

   int activeIterations();
}
