package org.infinispan.server.hotrod.iteration;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.filter.CacheFilters.filterAndConvert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.BaseCacheStream;
import org.infinispan.CacheStream;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.encoding.DataConversion;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.server.hotrod.CacheDecodeContext;
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
   final boolean metadata;
   final Function<Object, Object> resultFunction;

   IterationState(IterationSegmentsListener listener, Iterator<CacheEntry<Object, Object>> iterator, CacheStream<CacheEntry<Object, Object>> stream,
                  int batch, boolean metadata, Function<Object, Object> resultFunction) {
      this.listener = listener;
      this.iterator = iterator;
      this.stream = stream;
      this.batch = batch;
      this.metadata = metadata;
      this.resultFunction = resultFunction;
   }
}


public class DefaultIterationManager implements IterationManager {

   private static final Log log = LogFactory.getLog(DefaultIterationManager.class, Log.class);

   private final Map<String, IterationState> iterationStateMap = CollectionFactory.makeConcurrentMap();
   private final Map<String, KeyValueFilterConverterFactory> filterConverterFactoryMap =
         CollectionFactory.makeConcurrentMap();

   @Override
   public String start(CacheDecodeContext cdc, Optional<BitSet> segments, Optional<KeyValuePair<String, List<byte[]>>> namedFactory, int batch, boolean metadata) {
      String iterationId = Util.threadLocalRandomUUID().toString();

      AdvancedCache advancedCache = cdc.cache().getAdvancedCache();
      MediaType requestValueType = cdc.getHeader().getValueMediaType();
      DataConversion valueDataConversion = advancedCache.getValueDataConversion();
      Function<Object, Object> unmarshaller = p -> valueDataConversion.convert(p, requestValueType, APPLICATION_OBJECT);

      MediaType storageMediaType = advancedCache.getValueDataConversion().getStorageMediaType();

      IterationSegmentsListener segmentListener = new IterationSegmentsListener();
      CacheStream<CacheEntry<Object, Object>> stream;
      Stream<CacheEntry<Object, Object>> filteredStream;
      Function<Object, Object> resultTransformer = Function.identity();
      AdvancedCache iterationCache = advancedCache;
      if (!namedFactory.isPresent()) {
         stream = advancedCache.cacheEntrySet().stream();
         segments.map(bitSet -> stream.filterKeySegments(bitSet.stream().boxed().collect(Collectors.toSet())));
         filteredStream = stream.segmentCompletionListener(segmentListener);
      } else {

         KeyValueFilterConverterFactory factory = getFactory(namedFactory.get().getKey());
         List<byte[]> params = namedFactory.get().getValue();

         KeyValuePair<KeyValueFilterConverter, Boolean> filter = buildFilter(factory, params.toArray(new byte[params.size()][]), unmarshaller);
         KeyValueFilterConverter customFilter = filter.getKey();
         MediaType filterMediaType = customFilter.format();

         if (filterMediaType != null && filterMediaType.equals(storageMediaType)) {
            iterationCache = advancedCache.withEncoding(IdentityEncoder.class).withMediaType(filterMediaType.toString(), filterMediaType.toString());
         }
         stream = iterationCache.cacheEntrySet().stream();
         segments.map(bitSet -> stream.filterKeySegments(bitSet.stream().boxed().collect(Collectors.toSet())));
         IterationFilter iterationFilter = new IterationFilter(storageMediaType, requestValueType, Optional.of(filter.getKey()));
         filteredStream = filterAndConvert(stream.segmentCompletionListener(segmentListener), iterationFilter);
         if (filterMediaType != null && !storageMediaType.equals(requestValueType)) {
            resultTransformer = valueDataConversion::fromStorage;
         }
      }
      Iterator<CacheEntry<Object, Object>> iterator = filteredStream.iterator();

      IterationState iterationState = new IterationState(segmentListener, iterator, stream, batch, metadata, resultTransformer);

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

   private KeyValuePair<KeyValueFilterConverter, Boolean> buildFilter(KeyValueFilterConverterFactory factory, byte[][] params, Function<Object, Object> unmarshallParam) {
      if (factory instanceof ParamKeyValueFilterConverterFactory) {
         ParamKeyValueFilterConverterFactory paramFactory = (ParamKeyValueFilterConverterFactory) factory;
         Object[] unmarshallParams;
         if (paramFactory.binaryParam()) {
            unmarshallParams = params;
         } else {
            unmarshallParams = Arrays.stream(params).map(unmarshallParam).toArray();
         }
         return new KeyValuePair<>(paramFactory.getFilterConverter(unmarshallParams),
               paramFactory.binaryParam());
      } else {
         return new KeyValuePair<>(factory.getFilterConverter(), false);
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
               entries, iterationState.metadata, iterationState.resultFunction);
      } else {
         return new IterableIterationResult(Collections.emptySet(), OperationStatus.InvalidIteration,
               Collections.emptyList(), false, Function.identity());
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

}
