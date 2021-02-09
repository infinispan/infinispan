package org.infinispan.server.hotrod.iteration;

import java.util.BitSet;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.filter.KeyValueFilterConverterFactory;

/**
 * @author wburns
 * @since 9.0
 */
public interface IterationManager {
   IterationState start(AdvancedCache cache, BitSet segments,
                String filterConverterFactory, List<byte[]> filterConverterParams, MediaType valueMediaType, int batch, boolean metadata);

   IterableIterationResult next(String iterationId);

   IterationState close(String iterationId);

   void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory);

   void removeKeyValueFilterConverterFactory(String name);

   int activeIterations();
}
