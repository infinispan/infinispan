package org.infinispan.server.hotrod.iteration;

import static org.infinispan.filter.CacheFilters.filterAndConvert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.BaseCacheStream;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.OperationStatus;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.KeyValuePair;

/**
 * @author gustavonalle
 * @since 8.0
 */
class IterationSegmentsListener implements BaseCacheStream.SegmentCompletionListener {
   private Set<Integer> finished = new HashSet<>();
   private Set<Integer> justFinished = new HashSet<>();

   Set<Integer> getFinished(boolean endOfIteration) {
      synchronized (this) {
         if (endOfIteration) {
            Set<Integer> segments = new HashSet<>(finished);
            segments.addAll(justFinished);
            return segments;
         } else {
            Set<Integer> diff = new HashSet<>(finished);
            diff.removeAll(justFinished);
            finished.clear();
            finished.addAll(justFinished);
            return diff;
         }
      }
   }

   @Override
   public void segmentCompleted(Set<Integer> segments) {
      if (!segments.isEmpty()) {
         synchronized (this) {
            justFinished = segments;
            finished.addAll(segments);
         }
      }
   }
}

class IterationState {
   final IterationSegmentsListener listener;
   final Iterator<CacheEntry<Object, Object>> iterator;
   final CacheStream<CacheEntry<Object, Object>> stream;
   final int batch;
   final CompatInfo compatInfo;
   final boolean metadata;

   IterationState(IterationSegmentsListener listener, Iterator<CacheEntry<Object, Object>> iterator, CacheStream<CacheEntry<Object, Object>> stream,
                  int batch, CompatInfo compatInfo, boolean metadata) {
      this.listener = listener;
      this.iterator = iterator;
      this.stream = stream;
      this.batch = batch;
      this.compatInfo = compatInfo;
      this.metadata = metadata;
   }
}


class CompatInfo {
   final boolean enabled;
   final Encoder valueEncoder;

   CompatInfo(boolean enabled, Encoder valueEncoder) {
      this.enabled = enabled;
      this.valueEncoder = valueEncoder;
   }

   static CompatInfo create(CompatibilityModeConfiguration config) {
      return new CompatInfo(config.enabled(), config.enabled() ?
            new CompatModeEncoder(config.marshaller()) : IdentityEncoder.INSTANCE);
   }
}

public class DefaultIterationManager implements IterationManager {
   private final EmbeddedCacheManager cacheManager;

   volatile Optional<Marshaller> marshaller = Optional.empty();

   static final Log log = LogFactory.getLog(DefaultIterationManager.class, Log.class);

   private final Map<String, IterationState> iterationStateMap = CollectionFactory.makeConcurrentMap();
   private final Map<String, KeyValueFilterConverterFactory> filterConverterFactoryMap =
         CollectionFactory.makeConcurrentMap();

   public DefaultIterationManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public String start(Cache cache, Optional<BitSet> segments, Optional<KeyValuePair<String, List<byte[]>>> namedFactory, int batch, boolean metadata) {
      String iterationId = UUID.randomUUID().toString();
      AdvancedCache<Object, Object> advancedCache = cache.getAdvancedCache();
      CompatibilityModeConfiguration compatibilityConfig = advancedCache.getCacheConfiguration().compatibility();

      AdvancedCache<Object, Object> iterationCache = compatibilityConfig.enabled() ?
            (AdvancedCache<Object, Object>) advancedCache.withEncoding(IdentityEncoder.class) : advancedCache;

      CacheStream<CacheEntry<Object, Object>> stream = iterationCache.cacheEntrySet().stream();
      segments.map(bitSet -> stream.filterKeySegments(bitSet.stream().boxed().collect(Collectors.toSet())));

      IterationSegmentsListener segmentListener = new IterationSegmentsListener();
      CompatInfo compatInfo = CompatInfo.create(compatibilityConfig);

      Stream<CacheEntry<Object, Object>> filteredStream;
      if (namedFactory.isPresent()) {
         KeyValueFilterConverterFactory factory = getFactory(namedFactory.get().getKey());
         List<byte[]> params = namedFactory.get().getValue();
         KeyValuePair<KeyValueFilterConverter, Boolean> filter = buildFilter(factory, params.toArray(new byte[params.size()][]));
         IterationFilter iterationFilter = new IterationFilter(compatInfo.enabled, Optional.of(filter.getKey()), marshaller, filter.getValue());
         filteredStream = filterAndConvert(stream.segmentCompletionListener(segmentListener), iterationFilter);
      } else {
         filteredStream = stream.segmentCompletionListener(segmentListener);
      }

      IterationState iterationState = new IterationState(segmentListener, filteredStream.iterator(), stream, batch, compatInfo, metadata);
      iterationStateMap.put(iterationId, iterationState);
      return iterationId;
   }

   private KeyValueFilterConverterFactory getFactory(String name) {
      KeyValueFilterConverterFactory factory = filterConverterFactoryMap.get(name);
      if (factory == null) {
         throw log.missingKeyValueFilterConverterFactory(name);
      }
      return factory;
   }

   private KeyValuePair<KeyValueFilterConverter, Boolean> buildFilter(KeyValueFilterConverterFactory factory, byte[][] params) {
      if (factory instanceof ParamKeyValueFilterConverterFactory) {
         ParamKeyValueFilterConverterFactory paramFactory = (ParamKeyValueFilterConverterFactory) factory;
         Object[] unmarshallParams;
         if (paramFactory.binaryParam()) {
            unmarshallParams = params;
         } else {
            unmarshallParams = unmarshallParams(params, factory);
         }
         return new KeyValuePair<>(paramFactory.getFilterConverter(unmarshallParams),
               paramFactory.binaryParam());
      } else {
         return new KeyValuePair<>(factory.getFilterConverter(), false);
      }
   }

   private Object[] unmarshallParams(byte[][] params, Object factory) {
      Marshaller m = marshaller.orElseGet(() -> MarshallerBuilder.genericFromInstance(Optional.of(factory)));
      try {
         Object[] objectParams = new Object[params.length];
         int i = 0;
         for (byte[] param : params) {
            objectParams[i++] = m.objectFromByteBuffer(param);
         }
         return objectParams;
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }


   @Override
   public IterableIterationResult next(String cacheName, String iterationId) {
      IterationState iterationState = iterationStateMap.get(iterationId);
      if (iterationState != null) {
         int i = 0;
         List<CacheEntry> entries = new ArrayList<>(iterationState.batch);
         while (i++ < iterationState.batch && iterationState.iterator.hasNext()) {
            entries.add(iterationState.iterator.next());
         }
         return new IterableIterationResult(iterationState.listener.getFinished(entries.isEmpty()), OperationStatus.Success,
               entries, iterationState.compatInfo, iterationState.metadata);
      } else {
         return new IterableIterationResult(Collections.emptySet(), OperationStatus.InvalidIteration,
               Collections.emptyList(), null, false);
      }
   }

   @Override
   public boolean close(String cacheName, String iterationId) {
      IterationState iterationState = iterationStateMap.get(iterationId);
      if (iterationState != null) {
         iterationState.stream.close();
         iterationStateMap.remove(iterationId);
         return true;
      }
      return false;
   }

   @Override
   public void addKeyValueFilterConverterFactory(String name, KeyValueFilterConverterFactory factory) {
      filterConverterFactoryMap.put(name, factory);
   }

   @Override
   public void removeKeyValueFilterConverterFactory(String name) {
      filterConverterFactoryMap.remove(name);
   }

   @Override
   public int activeIterations() {
      return iterationStateMap.size();
   }

   @Override
   public void setMarshaller(Optional<Marshaller> marshaller) {
      this.marshaller = marshaller;
   }
}
