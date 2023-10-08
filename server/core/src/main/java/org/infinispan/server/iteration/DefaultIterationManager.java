package org.infinispan.server.iteration;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.filter.CacheFilters.filterAndConvert;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.BaseCacheStream;
import org.infinispan.CacheStream;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.time.TimeServiceTicker;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.encoding.DataConversion;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.OnChannelCloseReaper;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;

/**
 * @author gustavonalle
 * @since 8.0
 */
class IterationSegmentsListener implements BaseCacheStream.SegmentCompletionListener {
   private final Set<Integer> finished = new HashSet<>();
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

class DefaultIterationState implements IterationState, Closeable {
   final IterationSegmentsListener listener;
   final Iterator<CacheEntry<Object, Object>> iterator;
   final CacheStream<CacheEntry<Object, Object>> stream;
   final int batch;
   final boolean metadata;
   final Function<Object, Object> resultFunction;
   private final String id;
   private final OnChannelCloseReaper reaper;

   DefaultIterationState(String id, IterationSegmentsListener listener, Iterator<CacheEntry<Object, Object>> iterator, CacheStream<CacheEntry<Object, Object>> stream,
                         int batch, boolean metadata, Function<Object, Object> resultFunction, OnChannelCloseReaper reaper) {
      this.id = id;
      this.listener = listener;
      this.iterator = iterator;
      this.stream = stream;
      this.batch = batch;
      this.metadata = metadata;
      this.resultFunction = resultFunction;
      this.reaper = reaper;
   }

   @Override
   public void close() {
      stream.close();
      reaper.dispose();
   }

   @Override
   public String getId() {
      return id;
   }

   @Override
   public OnChannelCloseReaper getReaper() {
      return reaper;
   }
}


public class DefaultIterationManager implements IterationManager {

   private static final Log log = LogFactory.getLog(DefaultIterationManager.class, Log.class);

   private final com.github.benmanes.caffeine.cache.Cache<String, DefaultIterationState> iterationStateMap;
   private static final AtomicLong globalIterationId = new AtomicLong(0);
   private final Map<String, KeyValueFilterConverterFactory> filterConverterFactoryMap =
         new ConcurrentHashMap<>();

   public DefaultIterationManager(TimeService timeService) {
      Caffeine<Object, Object> builder = Caffeine.newBuilder();
      builder.expireAfterAccess(5, TimeUnit.MINUTES).removalListener(
            (RemovalListener<String, DefaultIterationState>) (key, value, cause) -> {
               value.close();
               if (cause.wasEvicted()) {
                  log.removedUnclosedIterator(key);
               }
            }).ticker(new TimeServiceTicker(timeService)).executor(new WithinThreadExecutor());
      iterationStateMap = builder.build();
   }

   @Override
   public IterationState start(AdvancedCache cache, BitSet segments, String filterConverterFactory, List<byte[]> filterConverterParams,
                               MediaType requestValueType, int batch, boolean metadata, DeliveryGuarantee guarantee, IterationInitializationContext ctx) {
      String iterationId = String.valueOf(globalIterationId.incrementAndGet());

      EmbeddedCacheManager cacheManager = SecurityActions.getEmbeddedCacheManager(cache);
      EncoderRegistry encoderRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager).getComponent(EncoderRegistry.class);
      DataConversion valueDataConversion = cache.getValueDataConversion();
      Function<Object, Object> unmarshaller = p -> encoderRegistry.convert(p, requestValueType, APPLICATION_OBJECT);

      // The iterator converts from the request type to the storage type. The filter will receive the data already converted.
      MediaType storageMediaType = valueDataConversion.getRequestMediaType() != null && valueDataConversion.getRequestMediaType() != MediaType.APPLICATION_UNKNOWN
            ? valueDataConversion.getRequestMediaType()
            : valueDataConversion.getStorageMediaType();

      IterationSegmentsListener segmentListener = new IterationSegmentsListener();
      CacheStream<CacheEntry<Object, Object>> stream;
      Stream<CacheEntry<Object, Object>> filteredStream;
      Function<Object, Object> resultTransformer = Function.identity();
      AdvancedCache iterationCache = cache;
      if (filterConverterFactory == null) {
         stream = baseStream(cache, ctx);
         if (segments != null) {
            stream.filterKeySegments(IntSets.from(segments.stream().iterator()));
         }
         filteredStream = stream.segmentCompletionListener(segmentListener);
      } else {
         KeyValueFilterConverterFactory factory = getFactory(filterConverterFactory);
         KeyValuePair<KeyValueFilterConverter, Boolean> filter = buildFilter(factory, filterConverterParams.toArray(Util.EMPTY_BYTE_ARRAY_ARRAY), unmarshaller);
         KeyValueFilterConverter customFilter = filter.getKey();
         MediaType filterMediaType = customFilter.format();

         if (filterMediaType != null && filterMediaType.equals(storageMediaType)) {
            iterationCache = cache.withMediaType(filterMediaType, filterMediaType);
         }
         stream = baseStream(iterationCache, ctx);
         if (segments != null) {
            stream.filterKeySegments(IntSets.from(segments.stream().iterator()));
         }
         IterationFilter iterationFilter = new IterationFilter(storageMediaType, requestValueType, (filter.getKey()));
         filteredStream = filterAndConvert(stream.segmentCompletionListener(segmentListener), iterationFilter);
         if (filterMediaType != null && !storageMediaType.equals(requestValueType)) {
            resultTransformer = valueDataConversion::fromStorage;
         }
      }
      Iterator<CacheEntry<Object, Object>> iterator = filteredStream.iterator();

      DefaultIterationState iterationState = new DefaultIterationState(iterationId, segmentListener, iterator, stream,
            batch, metadata, resultTransformer, new OnChannelCloseReaper(___ -> close(iterationId)));

      iterationStateMap.put(iterationId, iterationState);
      if (log.isTraceEnabled()) log.tracef("Started iteration %s", iterationId);
      return iterationState;
   }

   protected CacheStream<CacheEntry<Object, Object>> baseStream(AdvancedCache cache, IterationInitializationContext ctx) {
      return cache.cacheEntrySet().stream();
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
   public IterableIterationResult next(String iterationId, int batch) {
      DefaultIterationState iterationState = iterationStateMap.getIfPresent(iterationId);
      if (iterationState != null) {
         int i = 0;
         List<CacheEntry> entries;
         if (batch == Integer.MAX_VALUE) {
            entries = new ArrayList<>();
            while (iterationState.iterator.hasNext()) {
               entries.add(iterationState.iterator.next());
            }
         } else {
            // FIXME: if batch number is geant, OutOfMemory
            entries = new ArrayList<>();
            // FIXME: we are not using batch value here
            while (i++ < iterationState.batch && iterationState.iterator.hasNext()) {
               entries.add(iterationState.iterator.next());
            }
         }

         return new IterableIterationResult(iterationState.listener.getFinished(entries.isEmpty()), iterationState.iterator.hasNext() ? IterableIterationResult.Status.Success : IterableIterationResult.Status.Finished,
               entries, iterationState.metadata, iterationState.resultFunction);
      } else {
         return new IterableIterationResult(Collections.emptySet(), IterableIterationResult.Status.InvalidIteration,
               Collections.emptyList(), false, Function.identity());
      }
   }

   @Override
   public IterationState close(String iterationId) {
      DefaultIterationState iterationState = iterationStateMap.getIfPresent(iterationId);
      if (iterationState != null) {
         iterationStateMap.invalidate(iterationId);
      }
      if (log.isTraceEnabled()) log.tracef("Closed iteration %s", iterationId);
      return iterationState;
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
      iterationStateMap.cleanUp();
      return iterationStateMap.asMap().size();
   }
}
