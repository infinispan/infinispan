package org.infinispan.server.hotrod.iteration;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.util.KeyValuePair;

/**
 * @author wburns
 * @since 9.0
 */
public interface IterationManager {
   String start(String cacheName, Optional<BitSet> segments,
                Optional<KeyValuePair<String, List<byte[]>>> filterConverterFactory, int batch, boolean metadata);

   IterableIterationResult next(String cacheName, String iterationId);

   boolean close(String cacheName, String iterationId);

   void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory);

   void removeKeyValueFilterConverterFactory(String name);

   void setMarshaller(Optional<Marshaller> maybeMarshaller);

   int activeIterations();
}
