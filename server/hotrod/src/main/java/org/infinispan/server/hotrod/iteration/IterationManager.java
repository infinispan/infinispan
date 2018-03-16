package org.infinispan.server.hotrod.iteration;

import java.util.BitSet;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.filter.KeyValueFilterConverterFactory;

/**
 * @author wburns
 * @since 9.0
 */
public interface IterationManager {
   String start(Cache cache, BitSet segments,
                String filterConverterFactory, List<byte[]> filterConverterParams, MediaType valueMediaType, int batch, boolean metadata);

   IterableIterationResult next(String cacheName, String iterationId);

   boolean close(String cacheName, String iterationId);

   void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory);

   void removeKeyValueFilterConverterFactory(String name);

   int activeIterations();
}
