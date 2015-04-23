package org.infinispan.iteration.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterAsKeyFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * Entry retriever that only retrieves keys and values from the local stores.  This is useful for local, replicated
 * and invalidation caches.
 *
 * @author wburns
 * @since 7.0
 */
public class LocalEntryRetriever<K, V> implements EntryRetriever<K, V> {
   protected final Log log = LogFactory.getLog(this.getClass());
   protected final int batchSize;
   protected final long timeout;
   protected final TimeUnit unit;

   protected DataContainer<K, V> dataContainer;
   protected PersistenceManager persistenceManager;
   protected ExecutorService executorService;
   protected Cache<K, V> cache;
   protected ComponentRegistry componentRegistry;
   protected TimeService timeService;
   protected InternalEntryFactory entryFactory;
   protected Equivalence<K> keyEquivalence;

   protected final Executor withinThreadExecutor = new WithinThreadExecutor();
   protected final PartitionListener partitionListener = new PartitionListener();

   boolean passivationEnabled;

   @Inject
   public void inject(DataContainer<K, V> dataContainer, PersistenceManager persistenceManager,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
                      TimeService timeService, InternalEntryFactory entryFactory, Cache<K, V> cache,
                      Configuration config, ComponentRegistry componentRegistry) {
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.executorService = executorService;
      this.timeService = timeService;
      this.entryFactory = entryFactory;
      this.cache = cache;
      this.passivationEnabled = config.persistence().passivation();
      this.keyEquivalence = config.dataContainer().keyEquivalence();
      this.componentRegistry = componentRegistry;
   }

   @Start
   public void start() {
      cache.addListener(partitionListener);
   }

   @Listener
   protected class PartitionListener {
      protected volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

      protected final Set<Itr<?>> iterators = new ConcurrentHashSet<>();

      @PartitionStatusChanged
      public void onPartitionChange(PartitionStatusChangedEvent<K, V> event) {
         if (!event.isPre()) {
            currentMode = event.getAvailabilityMode();
            if (currentMode != AvailabilityMode.AVAILABLE) {
               Iterator<Itr<?>> itrIterator = iterators.iterator();
               // Now we close all the iterators but with the exception so they throw it properly
               while (itrIterator.hasNext()) {
                  Itr<?> itr = itrIterator.next();
                  itr.close(new AvailabilityException());
                  itrIterator.remove();
               }
            }
         }
      }
   }

   public LocalEntryRetriever(int batchSize, long timeout, TimeUnit unit) {
      if (batchSize <= 0) {
         throw new IllegalArgumentException("batchSize must be greater than 0");
      }
      if (timeout <= 0) {
         throw new IllegalArgumentException("timeout must be greater than 0");
      }
      if (unit == null) {
         throw new NullPointerException("unit must not be null");
      }
      this.batchSize = batchSize;
      this.timeout = timeout;
      this.unit = unit;
   }

   @Override
   public <C> void startRetrievingValues(UUID identifier, Address origin, Set<Integer> segments, Set<K> keysToFilter,
                                            KeyValueFilter<? super K, ? super V> filter,
                                            Converter<? super K, ? super V, C> converter, Set<Flag> flags) {
      throw new UnsupportedOperationException();
   }

   protected <C> void wireFilterAndConverterDependencies(KeyValueFilter<? super K, ? super V> filter, Converter<? super K, ? super V, C> converter) {
      if (filter != null) {
         componentRegistry.wireDependencies(filter);
      }
      if (converter != null && converter != filter) {
         componentRegistry.wireDependencies(converter);
      }
   }

   @Override
   public <C> void receiveResponse(UUID identifier, Address origin, Set<Integer> completedSegments, Set<Integer> inDoubtSegments,
                                   Collection<CacheEntry<K, C>> entries, CacheException e) {
      throw new UnsupportedOperationException();
   }

   @Listener
   protected static class PassivationListener<K, V> {
      Queue<K> activatedKeys = new ConcurrentLinkedQueue<K>();

      @CacheEntryActivated
      public void onEntryActivated(CacheEntryActivatedEvent<K, V> activatedEvent) {
         activatedKeys.add(activatedEvent.getKey());
      }
   }

   protected class KeyValueActionForCacheLoaderTask implements AdvancedCacheLoader.CacheLoaderTask<K, V> {

      private final BiConsumer<? super K, ? super InternalCacheEntry<K, V>> action;

      public KeyValueActionForCacheLoaderTask(BiConsumer<? super K, ? super InternalCacheEntry<K, V>> action) {
         this.action = action;
      }

      @Override
      public void processEntry(MarshalledEntry<K, V> marshalledEntry,
                               AdvancedCacheLoader.TaskContext taskContext)
            throws InterruptedException {
         if (!taskContext.isStopped()) {
            InternalMetadata metadata = marshalledEntry.getMetadata();
            if (metadata == null || !metadata.isExpired(timeService.wallClockTime())) {
               InternalCacheEntry<K, V> ice = PersistenceUtil.convert(marshalledEntry, entryFactory);
               action.accept(marshalledEntry.getKey(), ice);
            }
            if (Thread.interrupted()) {
               throw new InterruptedException();
            }
         }
      }
   }

   protected boolean shouldUseLoader(Set<Flag> flags) {
      return flags == null || !flags.contains(Flag.SKIP_CACHE_LOAD);
   }

   protected <C> void registerIterator(Itr<C> itr, Set<Flag> flags) {
      // If we are running in local mode we ignore the partition status
      if (flags == null || !flags.contains(Flag.CACHE_MODE_LOCAL)) {
         // Note we have to register the iterator before we check the mode in case if it changes concurrently
         partitionListener.iterators.add(itr);
         if (partitionListener.currentMode != AvailabilityMode.AVAILABLE) {
            partitionListener.iterators.remove(itr);
            throw log.partitionDegraded();
         }
      }
   }

   @Override
   public <C> CloseableIterator<CacheEntry<K, C>> retrieveEntries(final KeyValueFilter<? super K, ? super V> filter,
                                                                 final Converter<? super K, ? super V, ? extends C> converter,
                                                                 final Set<Flag> flags,
                                                                 final SegmentListener listener) {
      final boolean filterAndConvert;
      final Converter<? super K, ? super V, ? extends C> usedConverter;
      if (filter instanceof KeyValueFilterConverter && (filter == converter || converter == null)) {
         // perform filtering and conversion in a single step
         filterAndConvert = true;
         usedConverter = null;
         if (log.isTraceEnabled()) {
            log.tracef("User supplied a KeyValueFilterConverter for both filter and converter, so ignoring converter");
         }
      } else {
         filterAndConvert = false;
         usedConverter = converter;
      }
      wireFilterAndConverterDependencies(filter, usedConverter);
      // If we aren't using a loader just return the iterator which works on the data container directly
      if (flags != null && flags.contains(Flag.SKIP_CACHE_LOAD) || !cache.getCacheConfiguration().persistence().usingStores()) {
         // If we aren't in available mode (clustered only) and we aren't forcing the operation to be local only, then this 
         // operation can't continue since we are in a possible state of inconsistency.
         if ((flags == null || !flags.contains(Flag.CACHE_MODE_LOCAL)) && partitionListener.currentMode != AvailabilityMode.AVAILABLE) {
            throw log.partitionDegraded();
         }
         final Iterator<InternalCacheEntry<K, V>> iterator = dataContainer.iterator();

         return new DataContainerIterator<>(iterator, filter, usedConverter, filterAndConvert);
      }
      final Itr<C> iterator = new Itr<C>(batchSize);
      registerIterator(iterator, flags);
      final ItrQueuerHandler<C> handler = new ItrQueuerHandler<C>(iterator);
      executorService.submit(new Runnable() {

         @Override
         public void run() {
            try {
               final Set<K> processedKeys = CollectionFactory.makeSet(keyEquivalence);
               Queue<CacheEntry<K, C>> queue = new ArrayDeque<CacheEntry<K, C>>(batchSize) {
                  @Override
                  public boolean add(CacheEntry<K, C> kcEntry) {
                     processedKeys.add(kcEntry.getKey());
                     return super.add(kcEntry);
                  }
               };
               // Note we still use the batchSize here so that if we have a lot of values we return them as we see
               // them
               MapAction<C> action = new MapAction<>(batchSize, usedConverter, queue, handler);

               PassivationListener<K, V> listener = null;
               long currentTime = timeService.wallClockTime();
               try {
                  int interruptCheck = 0;
                  for (InternalCacheEntry<K, V> entry : dataContainer) {
                     if (!entry.isExpired(currentTime)) {
                        InternalCacheEntry<K, V> clone = entryFactory.create(unwrapMarshalledvalue(entry.getKey()),
                                                                             unwrapMarshalledvalue(entry.getValue()),
                                                                             entry);
                        K key = clone.getKey();
                        if (filter != null) {
                           if (filterAndConvert) {
                              C converted = ((KeyValueFilterConverter<K, V, C>)filter).filterAndConvert(
                                    key, clone.getValue(), clone.getMetadata());
                              if (converted != null) {
                                 clone.setValue((V) converted);
                              } else {
                                 continue;
                              }
                           }
                           else if (!filter.accept(key, clone.getValue(), clone.getMetadata())) {
                              continue;
                           }
                        }

                        action.accept(key, clone);
                        if (interruptCheck++ % batchSize == 0) {
                           if (Thread.interrupted()) {
                              throw new CacheException("Entry Iterator was interrupted!");
                           }
                        }
                     }
                  }
                  if (shouldUseLoader(flags) && persistenceManager.getStoresAsString().size() > 0) {
                     if (passivationEnabled) {
                        listener = new PassivationListener<K, V>();
                        cache.addListener(listener);
                     }
                     KeyFilter<K> loaderFilter;
                     if (filter == null || filterAndConvert) {
                        loaderFilter = new CollectionKeyFilter<K>(processedKeys);
                     } else {
                        loaderFilter = new CompositeKeyFilter<K>(new CollectionKeyFilter<K>(processedKeys),
                                                                 new KeyValueFilterAsKeyFilter<K>(filter));
                     }
                     if (filterAndConvert) {
                        action = new MapAction<>(batchSize, (KeyValueFilterConverter<K, V, C>) filter, queue, handler);
                     }
                     persistenceManager.processOnAllStores(withinThreadExecutor, loaderFilter,
                                                           new KeyValueActionForCacheLoaderTask(action), true, true);
                  }
               } finally {
                  if (listener != null) {
                     cache.removeListener(listener);
                     AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
                     // Now we have to check all the activated keys, as it is possible it got promoted to the
                     // in memory data container after we would have seen it
                     for (K key : listener.activatedKeys) {
                        // If we didn't process it already we have to look it up
                        if (!processedKeys.contains(key)) {
                           CacheEntry<K, V> entry = advancedCache.getCacheEntry(key);
                           if (entry != null) {
                              // We don't want to modify the entry itself
                              CacheEntry<K, V> clone = entry.clone();
                              if (filter != null) {
                                 if (filterAndConvert) {
                                    C converted = ((KeyValueFilterConverter<K, V, C>)filter).filterAndConvert(
                                          key, clone.getValue(), clone.getMetadata());
                                    if (converted != null) {
                                       clone.setValue((V) converted);
                                    } else {
                                       continue;
                                    }
                                 }
                                 else if (!filter.accept(key, clone.getValue(), clone.getMetadata())) {
                                    continue;
                                 }
                              }
                              action.accept(clone.getKey(), clone);
                           }
                        }
                     }
                  }
               }
               if (log.isTraceEnabled()) {
                  log.trace("Completed transfer of entries from cache");
               }

               handler.handleBatch(true, queue);
               partitionListener.iterators.remove(iterator);
            } catch (Throwable t) {
               CacheException e = log.exceptionProcessingEntryRetrievalValues(t);
               iterator.close(e);
            }
         }
      });
      return iterator;
   }

   private class MapAction<C> implements BiConsumer<K, CacheEntry<K, V>> {
      final Converter<? super K, ? super V, ? extends C> converter;
      final Queue<CacheEntry<K, C>> queue;
      final int batchSize;
      final BatchHandler<K, C> handler;

      final AtomicInteger insertionCount = new AtomicInteger();

      public MapAction(int batchSize, Converter<? super K, ? super V, ? extends C> converter, Queue<CacheEntry<K, C>> queue,
                       BatchHandler<K, C> handler)  {
         this.batchSize = batchSize;
         this.converter = converter;
         this.queue = queue;
         this.handler = handler;
      }

      @Override
      public void accept(K k, CacheEntry<K, V> kvInternalCacheEntry) {
         CacheEntry<K, C> clone = (CacheEntry<K, C>)kvInternalCacheEntry.clone();
         if (converter != null) {
            C value = converter.convert(k, kvInternalCacheEntry.getValue(), kvInternalCacheEntry.getMetadata());
            if (value == null && converter instanceof KeyValueFilterConverter) {  // the converter also acts as a filter here
               return;
            }
            clone.setValue(value);
         }

         // We use just an immortal cache entry since it has low serialization overhead
         queue.add(clone);
         if (insertionCount.incrementAndGet() % batchSize == 0) {
            try {
               handler.handleBatch(false, queue);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
            queue.clear();
         }
      }
   }

   protected interface BatchHandler<K, C> {
      public void handleBatch(boolean complete, Collection<CacheEntry<K, C>> entries) throws InterruptedException;
   }

   protected class DataContainerIterator<C> implements CloseableIterator<CacheEntry<K, C>> {
      private CacheEntry<K, C> next;
      private CacheEntry<K, C> prev;
      
      private final Iterator<InternalCacheEntry<K, V>> iterator;
      private final KeyValueFilter<? super K, ? super V> filter;
      private final Converter<? super K, ? super V, ? extends C> converter;
      private final boolean filterAndConvert;

      public DataContainerIterator(Iterator<InternalCacheEntry<K, V>> iterator, 
            KeyValueFilter<? super K, ? super V> filter,
            Converter<? super K, ? super V, ? extends C> converter,
            boolean filterAndConvert) {
         this.iterator = iterator;
         this.filter = filter;
         this.converter = converter;
         this.filterAndConvert = filterAndConvert;
      }

      @Override
      public boolean hasNext() {
         while (next == null) {
            if (iterator.hasNext()) {
               InternalCacheEntry<K, V> entry = iterator.next();
               if (log.isTraceEnabled()) {
                  log.tracef("Object [%s] returned from iteration - need to check if filtered", entry);
               }

               // Skip any expired entries
               if (entry.isExpired(timeService.wallClockTime())) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Object [%s] was expired, not returning", entry);
                  }
                  continue;
               }

               K marshalledKey = unwrapMarshalledvalue(entry.getKey());
               V marshalledValue = unwrapMarshalledvalue(entry.getValue());

               InternalCacheEntry<K, V> clone;
               if (marshalledKey != entry.getKey() || marshalledValue != entry.getValue()) {
                  clone = entryFactory.create(marshalledKey, marshalledValue, entry);
               } else {
                  clone = entry;
               }
               if (filter != null) {
                  K key = clone.getKey();
                  if (filterAndConvert) {
                     C converted = ((KeyValueFilterConverter<K, V, C>)filter).filterAndConvert(
                           key, clone.getValue(), clone.getMetadata());
                     if (converted != null) {
                        // If entry and clone are the same we didn't have a marshalled value then we need to create a 
                        // copy with the new value
                        if (entry == clone) {
                           clone = (InternalCacheEntry<K, V>) entryFactory.create(clone.getKey(), converted, entry);
                        } else {
                           clone.setValue((V) converted);
                        }
                     } else {
                        if (log.isTraceEnabled()) {
                           log.tracef("Object [%s] was filtered by KeyValueFilterConverter, not returning", clone);
                        }
                        continue;
                     }
                  }
                  else if (!filter.accept(key, clone.getValue(), clone.getMetadata())) {
                     if (log.isTraceEnabled()) {
                        log.tracef("Object [%s] was filtered, not returning", clone);
                     }
                     continue;
                  }
               }

               next = (CacheEntry<K, C>) clone;
               if (converter != null) {
                  C newValue = converter.convert(clone.getKey(), clone.getValue(), clone.getMetadata());
                  if (newValue != clone.getValue()) {
                     // If we didn't have a marshalled value then we need to create a copy with the new value
                     if (entry == clone) {
                        next = entryFactory.create(clone.getKey(), newValue, entry);
                     } else {
                        next.setValue(newValue);
                     }
                  }
               }
            } else {
               break;
            }
         }
         return next != null;
      }

      @Override
      public CacheEntry<K, C> next() {
         if (next == null && !hasNext()) {
            throw new NoSuchElementException();
         }
         CacheEntry<K, C> returnEntry = next;
         next = null;
         prev = returnEntry;
         return returnEntry;
      }

      @Override
      public void remove() {
         cache.remove(prev.getKey());
      }

      @Override
      public void close() {
         // Nothing to do here
      }
   }

   protected class ItrQueuerHandler<C> implements BatchHandler<K, C> {
      final Itr<C> iterator;

      public ItrQueuerHandler(Itr<C> iterator) {
         this.iterator = iterator;
      }

      @Override
      public void handleBatch(boolean complete, Collection<CacheEntry<K, C>> entries) throws InterruptedException {
         iterator.addEntries(entries);
         if (complete) {
            iterator.close();
         }
      }
   }

   protected class Itr<C> implements CloseableIterator<CacheEntry<K, C>> {

      private final BlockingQueue<CacheEntry<K, C>> queue;
      private final Lock nextLock = new ReentrantLock();
      private final Condition nextCondition = nextLock.newCondition();
      private boolean completed;
      private CacheException exception;

      public Itr(int batchSize) {
         // This is a blocking queue so that addEntries blocks to prevent multiple batches from the same sender
         this.queue = new ArrayBlockingQueue<>(batchSize);
      }

      @Override
      public boolean hasNext() {
         boolean hasNext = !queue.isEmpty();
         if (!hasNext) {
            boolean interrupted = false;
            long targetTime = timeService.expectedEndTime(timeout, unit);
            nextLock.lock();
            try {
               while (!(hasNext = !queue.isEmpty()) && !completed) {
                  try {
                     if (!nextCondition.await(timeService.remainingTime(targetTime, TimeUnit.NANOSECONDS),
                                              TimeUnit.NANOSECONDS)) {
                        if (log.isTraceEnabled()) {
                           log.tracef("Did not retrieve entries in allotted timeout: %s units: unit", timeout, unit);
                        }
                        throw new TimeoutException("Did not retrieve entries in allotted timeout: " + timeout +
                                                         " units: " + unit);
                     }
                  } catch (InterruptedException e) {
                     // If interrupted, we just loop back around
                     interrupted = true;
                  }
               }
               if (!hasNext && exception != null) {
                  throw exception;
               }
            } finally {
               nextLock.unlock();
            }

            if (interrupted) {
               Thread.currentThread().interrupt();
            }
         }
         return hasNext;
      }

      @Override
      public CacheEntry<K, C> next() {
         CacheEntry<K, C> entry = queue.poll();
         if (entry == null) {
            if (completed) {
               if (exception != null) {
                  throw exception;
               }
               throw new NoSuchElementException();
            }
            nextLock.lock();
            try {
               while ((entry = queue.poll()) == null && !completed) {
                  try {
                     nextCondition.await();
                  } catch (InterruptedException e) {
                     // If interrupted, we just loop back around
                  }
               }
               if (entry == null) {
                  if (exception != null) {
                     throw exception;
                  }
                  throw new NoSuchElementException();
               }
            } finally {
               nextLock.unlock();
            }
         }
         return entry;
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException("Remove is not supported!");
      }

      public void addEntries(Collection<CacheEntry<K, C>> entries) throws InterruptedException {
         boolean wasCompleted = completed;
         Iterator<CacheEntry<K, C>> itr = entries.iterator();
         while (!wasCompleted && itr.hasNext()) {
            // First we put as many as we can in the queue without wait blocking.  After it fills up or done we have to
            // signal others to wake up
            CacheEntry<K, C> entry = null;
            while (itr.hasNext()) {
               entry = itr.next();
               if (!queue.offer(entry)) {
                  break;
               }
               entry = null;
            }
            
            // Signal anyone waiting for values now
            nextLock.lock();
            try {
               wasCompleted = completed;
               nextCondition.signalAll();
            } finally {
               nextLock.unlock();
            }
            
            // If we broke out early from not fully iterating then we need to block until the queue has something
            // available to be inserted and then we restart again.
            if (entry != null) {
               while (!wasCompleted) {
                  // If we were able to offer a value this means someone took from the queue so let us come back around main
                  // loop to offer all values we can
                  if (queue.offer(entry, 100, TimeUnit.MILLISECONDS)) {
                     break;
                  }
                  // If we couldn't offer an entry check if we were completed concurrently
                  // We have to do in lock to ensure we see updated value properly
                  nextLock.lock();
                  try {
                     wasCompleted = completed;
                  } finally {
                     nextLock.unlock();
                  }
               }
            }
         }
      }

      @Override
      public void close() {
         close(null);
      }

      protected void close(CacheException e) {
         nextLock.lock();
         try {
            // We only set the exception if we weren't completed prior - which will allow for this iterator to complete
            // since it retrieved all the other values
            if (!completed) {
               exception = e;
               completed = true;
            }
            nextCondition.signalAll();
         } finally {
            nextLock.unlock();
         }
      }
   }

   protected static <T> T unwrapMarshalledvalue(T value) {
      if (value instanceof MarshalledValue) {
         return (T) ((MarshalledValue) value).get();
      }
      return value;
   }
}
