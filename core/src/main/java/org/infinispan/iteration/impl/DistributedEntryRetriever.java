package org.infinispan.iteration.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterAsKeyFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.filter.Converter;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.ConcurrentHashSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.infinispan.factories.KnownComponentNames.REMOTE_COMMAND_EXECUTOR;

/**
 * This is an entry retriever designed to retrieve values for a distributed cache.  This requests entries by segments
 * to further optimize when a rehash occurs to request less data each time.
 * <p>The way this works is when a new entry retriever is acquired it first calculates the remote node that has the
 * most segments and sends a request to it (this is done asynchronously).  Also another thread is spawned off
 * that processes the local data asynchronously.  When either is done (remote sends response) then it will process
 * the entries found and complete all segments that were marked as completed.  If it is a remote invocation then it
 * will send a new remote request to the node that now has the most remaining segments.  If it is local it will
 * complete those segments and stop running, unless a rehash has caused it to regain new local segments.  If a rehash
 * occurs during a remote node processing then those segments will automatically marked as suspect to not complete
 * them.</p>
 * <p>Values retrieved for suspect segments are sent to the iterator and it is noted that they were raised.  When
 * these segments are requested again the noted keys are sent along to reduce value payload size.</p>
 *
 * @author wburns
 * @since 7.0
 */
@Listener
public class DistributedEntryRetriever<K, V> extends LocalEntryRetriever<K, V> {
   private final AtomicReference<ConsistentHash> currentHash = new AtomicReference<ConsistentHash>();

   private DistributionManager distributionManager;
   private PersistenceManager persistenceManager;
   private CommandsFactory commandsFactory;
   private Address localAddress;
   private RpcManager rpcManager;
   private ExecutorService remoteExecutorService;

   private class IterationStatus<K, V, C> {
      private final DistributedItr<K, C> ongoingIterator;
      private final SegmentListener segmentListener;
      private final KeyValueFilter<? super K, ? super V> filter;
      private final Converter<? super K, ? super V, ? extends C> converter;
      private final Set<Flag> flags;
      private final AtomicReferenceArray<Set<K>> processedKeys;

      private final AtomicReference<Address> awaitingResponseFrom = new AtomicReference<>();
      private final AtomicReference<LocalStatus> localRunning = new AtomicReference<>(LocalStatus.IDLE);

      public IterationStatus(DistributedItr<K, C> ongoingIterator, SegmentListener segmentListener,
                              KeyValueFilter<? super K, ? super V> filter,
                              Converter<? super K, ? super V, ? extends C> converter,
                              Set<Flag> flags, AtomicReferenceArray<Set<K>> processedKeys) {
         this.ongoingIterator = ongoingIterator;
         this.segmentListener = segmentListener;
         this.filter = filter;
         this.converter = converter;
         this.flags = flags;
         this.processedKeys = processedKeys;
      }
   }

   private Map<UUID, IterationStatus<K, V, ? extends Object>> iteratorDetails = CollectionFactory.makeConcurrentMap();

   // This map keeps track of a listener when it is provided, this is useful to let caller know when a segment is
   // completed so they can do additional optimizations.  This is both used in local and remote iteration processing
   private ConcurrentMap<UUID, SegmentChangeListener> changeListener = CollectionFactory.makeConcurrentMap();

   private enum LocalStatus {
      RUNNING,
      REPEAT,
      IDLE
   }

   public DistributedEntryRetriever(int batchSize, long timeout, TimeUnit unit) {
      super(batchSize, timeout, unit);
   }

   /**
    * We need to listen to data rehash events in case if data moves while we are iterating over it.  This is both
    * important for the originator of the entry retriever request and remote nodes.  If a rehash occurs causing this
    * node to lose a segment and there is something iterating over the data container looking for values of that
    * segment, we can't guarantee that the data has all been seen correctly, so we must therefore suspect that node
    * and subsequently request it again from the new owner later.
    * @param event The data rehash event
    */
   @DataRehashed
   public void dataRehashed(DataRehashedEvent event) {
      ConsistentHash startHash = event.getConsistentHashAtStart();
      ConsistentHash endHash = event.getConsistentHashAtEnd();
      boolean trace = log.isTraceEnabled();
      if (event.isPre() && startHash != null && endHash != null) {
         log.tracef("Data rehash occurring startHash: %s and endHash: %s", startHash, endHash);

         if (!changeListener.isEmpty()) {
            if (trace) {
               log.tracef("Previous segments %s ", startHash.getPrimarySegmentsForOwner(localAddress));
               log.tracef("After segments %s ", endHash.getPrimarySegmentsForOwner(localAddress));
            }
            // we don't care about newly added segments, since that means our run wouldn't include them anyways
            Set<Integer> beforeSegments = new HashSet<Integer>(startHash.getPrimarySegmentsForOwner(localAddress));
            // Now any that were there before but aren't there now should be added - we don't care about new segments
            // since our current request shouldn't be working on it - it will have to retrieve it later
            beforeSegments.removeAll(endHash.getPrimarySegmentsForOwner(localAddress));
            if (!beforeSegments.isEmpty()) {
               // We have to make sure all current listeners get the newest hashes updated.  This has to occur for
               // new nodes and nodes leaving as the hash segments will change in both cases.
               for (Map.Entry<UUID, SegmentChangeListener> entry : changeListener.entrySet()) {
                  if (trace) {
                     log.tracef("Notifying %s through SegmentChangeListener", entry.getKey());
                  }
                  entry.getValue().changedSegments(beforeSegments);
               }
            } else if (trace) {
               log.tracef("No segments have been removed from data rehash, no notification required");
            }
         }
      }
   }

   /**
    * We need to listen for topology change events.  This is important for the originator so it can know when a node
    * goes down that it needs to now send the new request to the next remote node.  Also if the originator has
    * acquired some of the segments
    * @param event The topology change event
    */
   @TopologyChanged
   public void topologyChanged(TopologyChangedEvent event) {
      if (event.isPre()) {
         ConsistentHash beforeHash = event.getConsistentHashAtStart();
         ConsistentHash afterHash = event.getConsistentHashAtEnd();

         currentHash.set(afterHash);
         boolean trace = log.isTraceEnabled();

         if (beforeHash != null && afterHash != null) {
            if (trace) {
               log.tracef("Rehash hashes before %s after %s", beforeHash, afterHash);
            }
            Set<Address> leavers = new HashSet<Address>(beforeHash.getMembers());
            leavers.removeAll(afterHash.getMembers());
            if (!leavers.isEmpty() && trace) {
               log.tracef("Found leavers are %s", leavers);
            }

            for (Map.Entry<UUID, IterationStatus<K, V, ? extends Object>> details : iteratorDetails.entrySet()) {
               UUID identifier = details.getKey();
               IterationStatus<K, V, ? extends Object> status = details.getValue();
               Set<Integer> remoteSegments = findMissingRemoteSegments(status.processedKeys, afterHash);
               if (!remoteSegments.isEmpty()) {
                  Map.Entry<Address, Set<Integer>> route = findOptimalRoute(remoteSegments, afterHash);
                  boolean sendRequest;
                  AtomicReference<Address> awaitingResponsefrom = status.awaitingResponseFrom;
                  Address waitingFor = awaitingResponsefrom.get();
                  // If the node we are waiting from a response from has gone down we have to resubmit it - note we just
                  // call sendRequest without checking awaitingResponseFrom
                  if (sendRequest = leavers.contains(waitingFor)) {
                     if (trace) {
                        log.tracef("Resending new segment request %s for identifier %s since node %s has gone down",
                                   route.getValue(), identifier, waitingFor);
                     }
                  } else if (sendRequest = (waitingFor == null && awaitingResponsefrom.compareAndSet(null, route.getKey()))) {
                     // This clause is in case if we finished all remote segment retrievals and now we need to send
                     // a new one due to rehash
                     if (trace) {
                        log.tracef("There is no pending remote request for identifier %s, sending new one for segments %s",
                                   identifier, route.getValue());
                     }
                  }
                  if (sendRequest) {
                     if (status.ongoingIterator != null) {
                        // We don't have to call the eventuallySendRequest, because if the node we are sending to
                        // is now gone we will get another topology update and retry again - also this is async
                        // so we aren't blocking during an update
                        sendRequest(false, route, identifier, status);
                     } else {
                        // Just in case if we did the putIfAbsent to free up reference if the iterator was shutdown
                        awaitingResponsefrom.set(null);
                        if (trace) {
                           log.tracef("Not sending request since iterator has been closed for identifier %s", identifier);
                        }
                     }
                  }
               } else {
                  // If we get in here it means that all remaining segments are local - so we aren't waiting
                  // for a response any longer
                  details.getValue().awaitingResponseFrom.set(null);
               }


               Set<Integer> processSegments = findMissingLocalSegments(status.processedKeys, afterHash);
               if (!processSegments.isEmpty()) {
                  if (trace) {
                     log.tracef("Rehash caused our local node to acquire new segments %s for iteration %s processing",
                                processSegments, identifier);
                  }

                  startRetrievingValuesLocal(identifier, processSegments, status, new SegmentBatchHandler<K, Object>() {
                     @Override
                     public void handleBatch(UUID identifier, boolean complete, Set<Integer> completedSegments, Set<Integer> inDoubtSegments, Collection<CacheEntry<K, Object>> entries) {
                     processData(identifier, localAddress, completedSegments, inDoubtSegments, entries);
                     }
                  });
               }
            }
         }
      }
   }

   @Inject
   public void initialize(DistributionManager distributionManager,
                          PersistenceManager persistenceManager, CommandsFactory commandsFactory,
                          RpcManager rpcManager,
                          @ComponentName(REMOTE_COMMAND_EXECUTOR) ExecutorService remoteExecutorService) {
      this.distributionManager = distributionManager;
      this.persistenceManager = persistenceManager;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.remoteExecutorService = remoteExecutorService;
   }

   @Start
   public void start() {
      super.start();
      cache.addListener(this);
      localAddress = rpcManager.getAddress();
   }

   @Override
   public <C> void startRetrievingValues(final UUID identifier, final Address origin, final Set<Integer> segments,
                                        KeyValueFilter<? super K, ? super V> filter,
                                        Converter<? super K, ? super V, C> converter, Set<Flag> flags) {
      if (log.isTraceEnabled()) {
         log.tracef("Received entry request for %s from node %s for segments %s", identifier, origin, segments);
      }

      wireFilterAndConverterDependencies(filter, converter);

      startRetrievingValues(identifier, segments, filter, converter, flags, new SegmentBatchHandler<K, C>() {
         @Override
         public void handleBatch(UUID identifier, boolean complete, Set<Integer> completedSegments,
                                 Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> entries) {
            if (cache.getStatus() != ComponentStatus.RUNNING) {
               if (log.isTraceEnabled()) {
                  log.tracef("Cache status is no longer running, all segments are now suspect");
               }
               inDoubtSegments.addAll(completedSegments);
               completedSegments.clear();
            }
            if (log.isTraceEnabled()) {
               log.tracef("Sending batch response for %s to origin %s with %s completed segments, %s in doubt segments and %s values",
                          identifier, origin, completedSegments, inDoubtSegments, entries.size());
            }

            EntryResponseCommand command = commandsFactory.buildEntryResponseCommand(identifier, completedSegments,
                                                                                     inDoubtSegments, entries);
            rpcManager.invokeRemotely(Collections.singleton(origin), command, rpcManager.getRpcOptionsBuilder(
                  ResponseMode.SYNCHRONOUS).timeout(Long.MAX_VALUE, TimeUnit.SECONDS).build());
         }
      });
   }

   private <H, C extends H> void startRetrievingValues(final UUID identifier, final Set<Integer> segments,
                                         final KeyValueFilter<? super K, ? super V> filter,
                                         final Converter<? super K, ? super V, C> converter,
                                         final Set<Flag> flags, final SegmentBatchHandler<K, H> handler) {
      ConsistentHash hash = getCurrentHash();
      final Set<Integer> inDoubtSegments = new HashSet<>(segments.size());
      boolean canTryProcess = false;
      Iterator<Integer> iter = segments.iterator();
      while (iter.hasNext()) {
         Integer segment = iter.next();
         // If we still own any segments try to process
         if (localAddress.equals(hash.locatePrimaryOwnerForSegment(segment))) {
            canTryProcess = true;
         } else {
            inDoubtSegments.add(segment);
            iter.remove();
         }
      }
      if (canTryProcess) {
         executorService.execute(new Runnable() {

            @Override
            public void run() {
               Set<Integer> segmentsToUse = segments;
               Set<Integer> inDoubtSegmentsToUse = inDoubtSegments;
               ConsistentHash hashToUse = getCurrentHash();
               // this will stay as true for a local invocation until all local segments have been processed
               // a non local will set this to false at the end every time
               boolean repeat = true;
               while (repeat) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Starting retrieval of values for identifier %s", identifier);
                  }
                  SegmentChangeListener segmentChangeListener = new SegmentChangeListener();
                  changeListener.put(identifier, segmentChangeListener);
                  try {
                     final Set<K> processedKeys = CollectionFactory.makeSet(keyEquivalence);
                     Queue<CacheEntry<K, C>> queue = new ConcurrentLinkedQueue<CacheEntry<K, C>>() {
                        @Override
                        public boolean add(CacheEntry<K, C> kcEntry) {
                           processedKeys.add(kcEntry.getKey());
                           return super.add(kcEntry);
                        }
                     };
                     ParallelIterableMap.KeyValueAction<? super K, InternalCacheEntry<? super K, ? super V>> action =
                           new MapAction(identifier, segmentsToUse, inDoubtSegmentsToUse, batchSize, converter, handler,
                                         queue);

                     PassivationListener<K, V> listener = null;
                     long currentTime = timeService.wallClockTime();
                     try {
                        for (InternalCacheEntry<K, V> entry : dataContainer) {
                           if (!entry.isExpired(currentTime)) {
                              InternalCacheEntry<K, V> clone = entryFactory.create(unwrapMarshalledvalue(entry.getKey()),
                                                                                   unwrapMarshalledvalue(entry.getValue()), entry);
                              K key = clone.getKey();
                              if (filter != null) {
                                 if (converter == null && filter instanceof KeyValueFilterConverter) {
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
                              action.apply(key, clone);
                           }
                        }
                        if (shouldUseLoader(flags) && persistenceManager.getStoresAsString().size() > 0) {
                           KeyFilter<K> loaderFilter;
                           if (passivationEnabled) {
                              listener = new PassivationListener<K, V>();
                              cache.addListener(listener);
                           }
                           if (filter == null || converter == null && filter instanceof KeyValueFilterConverter) {
                              loaderFilter = new CompositeKeyFilter<K>(new SegmentFilter<K>(hashToUse, segmentsToUse),
                                                                       // We rely on this keeping a reference and not copying
                                                                       // contents
                                                                       new CollectionKeyFilter<K>(processedKeys));
                           } else {
                              loaderFilter = new CompositeKeyFilter<K>(new SegmentFilter<K>(hashToUse, segmentsToUse),
                                                                       new CollectionKeyFilter<K>(processedKeys),
                                                                       new KeyValueFilterAsKeyFilter<K>(filter));
                           }
                           if (converter == null && filter instanceof KeyValueFilterConverter) {
                              action = new MapAction(identifier, segmentsToUse, inDoubtSegmentsToUse, batchSize, (KeyValueFilterConverter) filter, handler, queue);
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
                                 CacheEntry entry = advancedCache.getCacheEntry(key);
                                 if (entry != null) {
                                    queue.add(entry);
                                 }
                              }
                           }
                        }
                     }
                     Set<Integer> completedSegments = new HashSet<Integer>();
                     for (Integer segment : segmentsToUse) {
                        if (localAddress.equals(getCurrentHash().locatePrimaryOwnerForSegment(segment)) &&
                              !segmentChangeListener.changedSegments.contains(segment)) {
                           // this segment should be complete then.
                           completedSegments.add(segment);
                        } else {
                           inDoubtSegmentsToUse.add(segment);
                        }
                     }
                     // No type to work around generics in sub sub types :)
                     Collection entriesToSend = new ArrayList<>(queue);

                     handler.handleBatch(identifier, true, completedSegments, inDoubtSegmentsToUse, entriesToSend);
                     if (log.isTraceEnabled()) {
                        log.tracef("Completed data iteration for request %s with segments %s", identifier, segmentsToUse);
                     }
                  } catch (Throwable e) {
                     log.exceptionProcessingEntryRetrievalValues(e);
                  } finally {
                     changeListener.remove(identifier);
                  }
                  repeat = shouldRepeatApplication(identifier);
                  if (repeat) {
                     // Only local would ever go into here
                     hashToUse = getCurrentHash();
                     IterationStatus<K, V, ? extends Object> status = iteratorDetails.get(identifier);
                     if (status != null) {
                        segmentsToUse = findMissingLocalSegments(status.processedKeys, hashToUse);
                        inDoubtSegmentsToUse.clear();

                        if (log.isTraceEnabled()) {
                           if (!segmentsToUse.isEmpty()) {
                                 log.tracef("Local retrieval found it should rerun - now finding segments %s for identifier %s",
                                            segmentsToUse, identifier);
                           } else {
                              log.tracef("Local retrieval for identifier %s was told to rerun - however no new segments " +
                                               "were found, looping around to try again", identifier);
                           }
                        }
                     } else {
                        log.tracef("Not repeating local retrieval since iteration was completed");
                        repeat = false;
                     }
                  } else {
                     if (log.isTraceEnabled()) {
                        log.tracef("Completed request %s for segments %s", identifier, segmentsToUse);
                     }
                     repeat = false;
                  }
               }
            }
         });
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Our node no longer has any of the segments %s that were requested for %s", inDoubtSegments,
                       identifier);
         }
         executorService.execute(new Runnable() {

            @Override
            public void run() {
               // If we don't have any of those segments just send back a response saying they are suspect with no values
               Collection<CacheEntry<K, H>> emptyEntries = Collections.emptySet();
               handler.handleBatch(identifier, true, segments, inDoubtSegments, emptyEntries);
            }
         });
      }
   }

   private <H, C extends H> void startRetrievingValuesLocal(final UUID identifier, final Set<Integer> segments,
                                                          IterationStatus<K, V, C> status,
                                                          final SegmentBatchHandler<K, H> handler) {
      boolean shouldRun = updatedLocalAndRun(identifier);
      if (shouldRun) {
         if (log.isTraceEnabled()) {
            log.tracef("Starting local request to retrieve segments %s for identifier %s", segments, identifier);
         }
         startRetrievingValues(identifier, segments, status.filter, status.converter, status.flags, handler);
      } else if (log.isTraceEnabled()) {
         log.tracef("Not running local retrieval as another thread is handling it for identifier %s.", identifier);
      }
   }

   @Override
   public <C> CloseableIterator<CacheEntry<K, C>> retrieveEntries(KeyValueFilter<? super K, ? super V> filter,
                                                    Converter<? super K, ? super V, ? extends C> converter,
                                                    Set<Flag> flags,
                                                    SegmentListener listener) {
      // If we are marked as local don't process distributed entries
      if (flags != null && flags.contains(Flag.CACHE_MODE_LOCAL)) {
         log.trace("Skipping distributed entry retrieval and processing local only as CACHE_MODE_LOCAL flag was set");
         return super.retrieveEntries(filter, converter, flags, listener);
      }

      ConsistentHash hash = getCurrentHash();
      // If we aren't in the hash then just run the command locally
      if (!hash.getMembers().contains(localAddress)) {
         log.trace("Skipping distributed entry retrieval and processing local since we are not part of the consistent hash");
         return super.retrieveEntries(filter, converter, flags, listener);
      }

      UUID identifier = UUID.randomUUID();
      final Converter<? super K, ? super V, ? extends C> usedConverter = checkForKeyValueFilterConverter(filter,
                                                                                                         converter);
      if (log.isTraceEnabled()) {
         log.tracef("Processing entry retrieval request with identifier %s with filter %s and converter %s", identifier,
                    filter, usedConverter);
      }

      DistributedItr<K, C> itr = new DistributedItr<>(batchSize, identifier, listener, hash);
      registerIterator(itr, flags);
      Set<Integer> remoteSegments = new HashSet<>();
      AtomicReferenceArray<Set<K>> processedKeys = new AtomicReferenceArray<Set<K>>(hash.getNumSegments());
      for (int i = 0; i < processedKeys.length(); ++i) {
         // Normally we only work on a single segment per thread.  But since there is an edge case where
         // a node that has left can still send a response, we need this to be a CHS.
         processedKeys.set(i, new ConcurrentHashSet<K>());
         remoteSegments.add(i);
      }

      IterationStatus status = new IterationStatus<>(itr, listener, filter, usedConverter, flags, processedKeys);
      iteratorDetails.put(identifier, status);

      Set<Integer> ourSegments = hash.getPrimarySegmentsForOwner(localAddress);
      remoteSegments.removeAll(ourSegments);
      if (!remoteSegments.isEmpty()) {
         eventuallySendRequest(identifier, status);
      }
      if (!ourSegments.isEmpty()) {
         wireFilterAndConverterDependencies(filter, usedConverter);
         startRetrievingValuesLocal(identifier, ourSegments, status, new SegmentBatchHandler<K, C>() {
            @Override
            public void handleBatch(UUID identifier, boolean complete, Set<Integer> completedSegments, Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> entries) {
               processData(identifier, localAddress, completedSegments, inDoubtSegments, entries);
            }
         });
      }
      return itr;
   }

   private ConsistentHash getCurrentHash() {
      ConsistentHash hash = currentHash.get();
      if (hash == null) {
         currentHash.compareAndSet(null, distributionManager.getReadConsistentHash());
         hash = currentHash.get();
      }
      return hash;
   }

   private <C> boolean eventuallySendRequest(UUID identifier, IterationStatus<K, V, ? extends Object> status) {
      boolean sent = false;
      while (!sent) {
         // This means our iterator was closed explicitly
         if (!iteratorDetails.containsKey(identifier)) {
            if (log.isTraceEnabled()) {
               log.tracef("Cannot send remote request as our iterator was concurrently closed for %s", identifier);
            }
            return false;
         }

         ConsistentHash hash = getCurrentHash();
         Set<Integer> missingRemoteSegments = findMissingRemoteSegments(status.processedKeys, hash);
         if (!missingRemoteSegments.isEmpty()) {
            Map.Entry<Address, Set<Integer>> route = findOptimalRoute(missingRemoteSegments, hash);

            // If another request came in we don't want to keep on trying.  This could happen if a rehash caused
            // our existing node request to go away.
            if (status.awaitingResponseFrom.compareAndSet(null, route.getKey())) {
               sent = sendRequest(true, route, identifier, status);
            } else {
               break;
            }
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Cannot send remote request as there are no longer any remote segments missing for %s", identifier);
            }
            break;
         }
      }
      return sent;
   }

   private <C> boolean sendRequest(boolean sync, Map.Entry<Address, Set<Integer>> route, final UUID identifier,
                                   IterationStatus<K, V, ? extends Object> status) {
      if (log.isTraceEnabled()) {
         log.tracef("Sending request to %s for identifier %s", route, identifier);
      }
      Address address = route.getKey();
      status.awaitingResponseFrom.set(address);
      Set<Integer> segments = route.getValue();
      Set<K> keysToFilter = new HashSet<K>();

      AtomicReferenceArray<Set<K>> ourEntries = status.processedKeys;
      for (Integer segment : segments) {
         Set<K> valuesSeen = ourEntries.get(segment);
         if (valuesSeen != null) {
            keysToFilter.addAll(valuesSeen);
         }
      }
      KeyValueFilter<? super K, ? super V> filterToSend;
      if (status.filter == null) {
         if (!keysToFilter.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("Applying filter for %s of keys", keysToFilter.size());
            }
            filterToSend = new KeyFilterAsKeyValueFilter<K, V>(new CollectionKeyFilter<K>(keysToFilter));
         } else {
            if (log.isTraceEnabled()) {
               log.trace("No filter applied");
            }
            filterToSend = null;
         }
      } else {
         if (!keysToFilter.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("Applying filter for %s of keys with provided filter %s" , keysToFilter.size(), status.filter);
            }
            filterToSend = new CompositeKeyValueFilter<K, V>(
                  new KeyFilterAsKeyValueFilter<K, V>(new CollectionKeyFilter<K>(keysToFilter)), status.filter);
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("Using provided filter %s", status.filter);
            }
            filterToSend = status.filter;
         }
      }

      EntryRequestCommand<K, V, ? extends Object> command = commandsFactory.buildEntryRequestCommand(identifier, segments,
                                                                                      filterToSend, status.converter,
                                                                                      status.flags);
      try {
         // We don't want async with sync marshalling as we don't want the extra overhead time
         RpcOptions options = rpcManager.getRpcOptionsBuilder(sync ? ResponseMode.SYNCHRONOUS :
                                                                    ResponseMode.ASYNCHRONOUS).build();
         Map<Address, Response> responseMap = rpcManager.invokeRemotely(Collections.singleton(address), command, options);
         if (sync) {
            Response response = responseMap.values().iterator().next();
            if (!response.isSuccessful()) {
               Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
               if (log.isTraceEnabled()) {
                  log.tracef(cause, "Unsuccessful response received from node %s for %s, must resend to a new node!",
                             route.getKey(), identifier);
               }
               atomicRemove(status.awaitingResponseFrom, address);
               return false;
            }
         }
         return true;
      } catch (SuspectException e) {
         if (log.isTraceEnabled()) {
            log.tracef("Request to %s for %s was suspect, must resend to a new node!", route, identifier);
         }
         atomicRemove(status.awaitingResponseFrom, address);
         return false;
      }
   }

   private Set<Integer> findMissingLocalSegments(AtomicReferenceArray<Set<K>> processValues, ConsistentHash hash) {
      Set<Integer> ourSegments = hash.getPrimarySegmentsForOwner(localAddress);
      Set<Integer> returnSegments = new HashSet<>();
      for (Integer segment : ourSegments) {
         if (processValues.get(segment) != null) {
            returnSegments.add(segment);
         }
      }

      return returnSegments;
   }

   private boolean updatedLocalAndRun(UUID identifier) {
      boolean shouldRun = false;
      boolean updated = false;
      IterationStatus<K, V, ?> details = iteratorDetails.get(identifier);
      if (details != null) {
         AtomicReference<LocalStatus> localRunning = details.localRunning;
         while (!updated) {
            LocalStatus status = localRunning.get();
            // Just ignore a null status
            if (status == null) {
               updated = true;
            }
            switch (status) {
               case IDLE:
                  // If idle we try to update to running which means we should fire it off
                  updated = shouldRun = localRunning.compareAndSet(LocalStatus.IDLE, LocalStatus.RUNNING);
                  break;
               case REPEAT:
                  // If is repeat then we don't worry since it will have to be repeated still
                  updated = true;
                  break;
               case RUNNING:
                  // If it is running try to set to repeat to make sure they know about the new segments
                  updated = localRunning.compareAndSet(LocalStatus.RUNNING, LocalStatus.REPEAT);
                  break;
            }
         }
      }
      return shouldRun;
   }

   private boolean shouldRepeatApplication(UUID identifier) {
      boolean shouldRun = false;
      boolean updated = false;
      IterationStatus<K, V, ?> details = iteratorDetails.get(identifier);
      if (details != null) {
         AtomicReference<LocalStatus> localRunning = details.localRunning;
         while (!updated) {
            LocalStatus status = localRunning.get();
            if (status == null) {
               throw new IllegalStateException("Status should never be null");
            } else {
               switch (status) {
                  case IDLE:
                     throw new IllegalStateException("This should never be seen as IDLE by the running thread");
                  case REPEAT:
                     updated = shouldRun = localRunning.compareAndSet(LocalStatus.REPEAT, LocalStatus.RUNNING);
                     break;
                  case RUNNING:
                     updated = localRunning.compareAndSet(LocalStatus.RUNNING, LocalStatus.IDLE);
                     break;
               }
            }
         }
      }
      return shouldRun;
   }

   private boolean missingRemoteSegment(AtomicReferenceArray<Set<K>> processValues, ConsistentHash hash) {
      boolean missingRemote = false;
      if (processValues != null) {
         Set<Integer> localSegments = hash.getPrimarySegmentsForOwner(localAddress);
         for (int i = 0; i < processValues.length(); ++i) {
            if (processValues.get(i) != null) {
               if (!localSegments.contains(i)) {
                  missingRemote = true;
                  break;
               }
            }
         }
      }
      return missingRemote;
   }

   private Set<Integer> findMissingRemoteSegments(AtomicReferenceArray<Set<K>> processValues, ConsistentHash hash) {
      Set<Integer> localSegments = hash.getPrimarySegmentsForOwner(localAddress);
      Set<Integer> segments = new HashSet<>();
      if (processValues != null) {
         for (int i = 0; i < processValues.length(); ++i) {
            if (processValues.get(i) != null) {
               if (!localSegments.contains(i)) {
                  segments.add(i);
               }
            }
         }
      }
      return segments;
   }

   /**
    * Finds the address with the most amount of segments to request and returns it - note this will never return
    * the local address
    * @param segmentsToFind The segments to find or null if all segments are desired
    * @return
    */
   private Map.Entry<Address, Set<Integer>> findOptimalRoute(Set<Integer> segmentsToFind, ConsistentHash hash) {
      Map.Entry<Address, Set<Integer>> route = null;
      Map<Address, Set<Integer>> routes;
      int segmentCount = hash.getNumSegments();
      routes = new HashMap<>();
      for (int i = 0; i < segmentCount; ++i) {
         if (segmentsToFind == null || segmentsToFind.contains(i)) {
            Address address = hash.locatePrimaryOwnerForSegment(i);
            Set<Integer> segments = routes.get(address);
            if (segments == null) {
               segments = new HashSet<>();
               routes.put(address, segments);
            }
            segments.add(i);
         }
      }
      for (Map.Entry<Address, Set<Integer>> mappedRoute : routes.entrySet()) {
         if (mappedRoute.getKey().equals(localAddress)) {
            continue;
         }
         if (route == null) {
            route = mappedRoute;
         } else if (route.getValue().size() > mappedRoute.getValue().size()) {
            route = mappedRoute;
         }
      }
      return route;
   }


   @Override
   public <C> void receiveResponse(UUID identifier, Address origin, Set<Integer> completedSegments,
                                   Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> entries) {
      if (log.isTraceEnabled()) {
         log.tracef("Processing response for identifier %s", identifier);
      }
      try {
         processData(identifier, origin, completedSegments, inDoubtSegments, entries);
      } catch (Exception e) {
         log.exceptionProcessingIteratorResponse(identifier, e);
      }
   }

   /**
    * This method is only called on the originator node to process values either retrieved remotely or locally.
    * After processing data this method then determines if it needs to send another request to another remote node
    * and also if it needs to do another local data mine in case if the topology changed.
    * @param origin Where the data request came from
    * @param identifier The unique identifier for this iteration request
    * @param completedSegments The segments that were completed
    * @param inDoubtSegments The segments that were found to be in doubt due to a rehash while iterating over them
    * @param entries The entries retrieved
    * @param <C> The type for the entries value
    */
   private <C> void processData(final UUID identifier, Address origin, Set<Integer> completedSegments, Set<Integer> inDoubtSegments,
                            Collection<CacheEntry<K, C>> entries) {
      final IterationStatus<K, V, C> status = (IterationStatus<K, V, C>) iteratorDetails.get(identifier);
      // This is possible if the iterator was closed early or we had duplicate requests due to a rehash.
      if (status != null) {
         final AtomicReferenceArray<Set<K>> processedKeys = status.processedKeys;

         DistributedItr<K, C> itr = status.ongoingIterator;
         if (log.isTraceEnabled()) {
            log.tracef("Processing data for identifier %s completedSegments: %s inDoubtSegments: %s entryCount: %s", identifier,
                       completedSegments, inDoubtSegments, entries.size());
         }
         // Normally we shouldn't have duplicates, but rehash can cause that
         Collection<CacheEntry<K, C>> nonDuplicateEntries = new ArrayList<>(entries.size());
         Map<Integer, ConcurrentHashSet<K>> finishedKeysForSegment = new HashMap<>();
         // We have to put the empty hash set, or else segments with no values would complete
         for (int completedSegment : completedSegments) {
            // Only notify segments that have completed once! Technically this can still occur twice, since the
            // segments aren't completed until later, but this happening is not an issue since we only raise a key once,
            // but this is here to reduce tracing output and false positives in tests.
            if (processedKeys.get(completedSegment) != null) {
               finishedKeysForSegment.put(completedSegment, new ConcurrentHashSet<K>());
            }
         }
         // We need to keep track of what we have seen in case if they become in doubt
         ConsistentHash hash = getCurrentHash();
         for (CacheEntry<K, C> entry : entries) {
            K key = entry.getKey();
            int segment = hash.getSegment(key);
            Set<K> seenSet = processedKeys.get(segment);
            // If the set is null means that this segment was already finished... so don't worry about those values
            if (seenSet != null) {
               // If we already saw the value don't raise it again
               if (seenSet.add(key)) {
                  ConcurrentHashSet<K> finishedKeys = finishedKeysForSegment.get(segment);
                  if (finishedKeys != null) {
                     finishedKeys.add(key);
                  }
                  nonDuplicateEntries.add(entry);
               }
            }
         }

         itr.addKeysForSegment(finishedKeysForSegment);

         try {
            itr.addEntries(nonDuplicateEntries);
         } catch (InterruptedException e) {
            if (log.isTraceEnabled()) {
               // If we were interrupted then just shut down this processing completely
               log.tracef("Iteration thread was interrupted, stopping iteration for identifier %s", identifier);
            }
            completeIteration(identifier);
         }

         // We complete the segments after setting the entries
         if (!completedSegments.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("Completing segments %s for identifier %s", completedSegments, identifier);
            }
            for (Integer completeSegment : completedSegments) {
               // Null out the set saying we completed this segment
               processedKeys.set(completeSegment, null);
            }
         }

         // If we are finished we need to request the next segments - currently the indoubt and completed are
         // sent at the end
         // Also don't check completion if we know we are waiting for another node to respond
         if (!completedSegments.isEmpty() || !inDoubtSegments.isEmpty()) {
            boolean complete = true;
            // We have to use the same has for both local and remote just in case - note both will check the updated
            // hash later
            hash = getCurrentHash();

            boolean isMissingRemoteSegments = missingRemoteSegment(processedKeys, hash);
            if (isMissingRemoteSegments) {
               if (log.isTraceEnabled()) {
                  // Note if a rehash occurs here and all our segments become local this could be an empty set
                  log.tracef("Request %s not yet complete, remote segments %s are still missing", identifier,
                             findMissingRemoteSegments(processedKeys, hash));
               }
               complete = false;
               if (origin != localAddress) {
                  // Only perform if the awaitingResponse is still not null, which is our current response.  If it
                  // is null that means the iterator was closed, if it was non null means this node went down while
                  // processing response
                  if (atomicRemove(status.awaitingResponseFrom, origin)) {
                     if (log.isTraceEnabled()) {
                        log.tracef("Sending request for %s via remote transport thread", identifier);
                     }
                     remoteExecutorService.submit(new Runnable() {
                        @Override
                        public void run() {
                           eventuallySendRequest(identifier, status);
                        }
                     });
                  } else if (log.isTraceEnabled()) {
                     log.tracef("Not sending new remote request as %s was either stopped or %s went down", identifier,
                                origin);
                  }
               }
            } else if (origin != localAddress) {
               // If we don't have another node to send to mark the response as no longer required
               status.awaitingResponseFrom.set(null);

               remoteExecutorService.submit(new Runnable() {
                  @Override
                  public void run() {
                     // We have to keep trying until either there are no more missing segments or we have sent a request
                     while (missingRemoteSegment(processedKeys, getCurrentHash()) && iteratorDetails.containsKey(identifier)) {
                        if (!eventuallySendRequest(identifier, status)) {
                           // We couldn't send a remote request, so remove the awaitingResponse and make sure there
                           // are no more missing remote segments
                           status.awaitingResponseFrom.set(null);
                        } else {
                           // This means we successfully sent the request so our job is done!
                           break;
                        }
                     }
                  }
               });
            }

            Set<Integer> localSegments = findMissingLocalSegments(processedKeys, hash);
            if (!localSegments.isEmpty()) {
               if (log.isTraceEnabled()) {
                  log.tracef("Request %s not yet complete, local segments %s are still missing", identifier, localSegments);
               }
               complete = false;
               // Have the local request check it's values again
               startRetrievingValuesLocal(identifier, localSegments, status, new SegmentBatchHandler<K, Object>() {
                  @Override
                  public void handleBatch(UUID identifier, boolean complete, Set<Integer> completedSegments, Set<Integer> inDoubtSegments, Collection<CacheEntry<K, Object>> entries) {
                     processData(identifier, localAddress, completedSegments, inDoubtSegments, entries);
                  }
               });
            }

            if (complete) {
               completeIteration(identifier);
            }
         }
      } else if (log.isTraceEnabled()) {
         log.tracef("Ignoring values as identifier %s was marked as complete", identifier);
      }
   }

   private static <V> boolean atomicRemove(AtomicReference<V> ref, V object) {
      V refObject = ref.get();
      if (object.equals(refObject)) {
         return ref.compareAndSet(refObject, null);
      } else {
         return false;
      }
   }

   private static <V> boolean atomicReplace(AtomicReference<V> ref, V object, V newObject) {
      V refObject = ref.get();
      if (object.equals(refObject)) {
         return ref.compareAndSet(refObject, newObject);
      } else {
         return false;
      }
   }

   private void completeIteration(UUID identifier) {
      if (log.isTraceEnabled()) {
         log.tracef("Processing complete for identifier %s", identifier);
      }
      IterationStatus<K, V, ?> status = iteratorDetails.get(identifier);
      if (status != null) {
         Itr<K, ?> itr = status.ongoingIterator;
         partitionListener.iterators.remove(itr);
         itr.close();
      }
   }

   protected class DistributedItr<K, C> extends Itr<K, C> {
      private final UUID identifier;
      private final ConsistentHash hash;
      private final ConcurrentMap<Integer, Set<K>> keysNeededToComplete = new ConcurrentHashMap<>();
      private final SegmentListener segmentListener;

      public DistributedItr(int batchSize, UUID identifier, SegmentListener segmentListener, ConsistentHash hash) {
         super(batchSize);
         this.identifier = identifier;
         this.hash = hash;
         this.segmentListener = segmentListener;
      }

      @Override
      public CacheEntry<K, C> next() {
         CacheEntry<K, C> entry = super.next();
         K key = entry.getKey();
         int segment = hash.getSegment(key);
         Set<K> keys = keysNeededToComplete.get(segment);
         if (keys != null) {
            keys.remove(key);
            if (keys.isEmpty()) {
               notifyListenerCompletedSegment(segment, true);
            }
         }
         return entry;
      }

      private void notifyListenerCompletedSegment(int segment, boolean fromIterator) {
         if (segmentListener != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Notifying listener of segment %s being completed for %s", segment, identifier);
            }
            segmentListener.segmentTransferred(segment, fromIterator);
         }
      }

      public void addKeysForSegment(Map<Integer, ConcurrentHashSet<K>> keysForSegment) {
         for (Map.Entry<Integer, ConcurrentHashSet<K>> entry : keysForSegment.entrySet()) {
            Set<K> values = entry.getValue();
            // If it is empty just notify right away
            if (values.isEmpty()) {
               // If we have keys to be notified, then don't complete the segment due to this response having no valid
               // keys.  This means a previous response came for this segment that had keys.
               if (!keysNeededToComplete.containsKey(entry.getKey())) {
                  notifyListenerCompletedSegment(entry.getKey(), false);
               } else {
                  if (log.isTraceEnabled()) {
                     log.tracef("No keys found for segment %s, but previous response had keys - so cannot complete " +
                                      "segment", entry.getKey());
                  }
               }
            }
            // Else we have to wait until we iterate over the values first
            else {
               Set<K> prevValues = keysNeededToComplete.putIfAbsent(entry.getKey(), values);
               if (prevValues != null) {
                  // Can't use addAll due to CHS impl
                  for (K value : values) {
                     prevValues.add(value);
                  }
               }
            }
         }
      }

      protected void close(CacheException e) {
         super.close(e);
         // When the iterator is closed we have to stop all other processing and remove any references to our identifier
         iteratorDetails.remove(identifier);
      }

      @Override
      protected void finalize() throws Throwable {
         super.finalize();
         close();
      }
   }

   private class MapAction<C> implements ParallelIterableMap.KeyValueAction<K, InternalCacheEntry<K, V>> {
      final UUID identifier;
      final Set<Integer> segments;
      final int batchSize;
      final Converter<? super K, ? super V, C> converter;
      final SegmentBatchHandler<K, C> handler;
      final Queue<CacheEntry<K, C>> queue;

      final AtomicInteger insertionCount = new AtomicInteger();

      public MapAction(UUID identifier, Set<Integer> segments, Set<Integer> inDoubtSegments,
                       int batchSize, Converter<? super K, ? super V, C> converter, SegmentBatchHandler<K, C> handler,
                       Queue<CacheEntry<K, C>> queue)  {
         this.identifier = identifier;
         this.segments = segments;
         this.batchSize = batchSize;
         this.converter = converter;
         this.handler = handler;
         this.queue = queue;
      }

      @Override
      public void apply(K k, InternalCacheEntry<K, V> kvInternalCacheEntry) {
         ConsistentHash hash = getCurrentHash();
         if (segments.contains(hash.getSegment(k))) {
            CacheEntry<K, C> clone = (CacheEntry<K, C>)kvInternalCacheEntry.clone();
            if (converter != null) {
               C value = converter.convert(k, kvInternalCacheEntry.getValue(), kvInternalCacheEntry.getMetadata());
               if (value == null && converter instanceof KeyValueFilterConverter) {
                  return;
               }
               clone.setValue(value);
            }
            queue.add(clone);
            if (insertionCount.incrementAndGet() % batchSize == 0) {
               Collection<CacheEntry<K, C>> entriesToSend = new ArrayList<>(batchSize);
               while (entriesToSend.size() != batchSize) {
                  entriesToSend.add(queue.poll());
               }

               Set<Integer> emptySet = Collections.emptySet();
               // We always send back empty set for both completed and in doubt segments
               handler.handleBatch(identifier, false, emptySet, emptySet, entriesToSend);
            }
         }
      }
   }

   private interface SegmentBatchHandler<K, C> {
      public void handleBatch(UUID identifier, boolean complete, Set<Integer> completedSegments,
                              Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> entries);
   }

   private static class SegmentFilter<K> implements KeyFilter<K> {
      private final ConsistentHash hash;
      private final Set<Integer> segments;

      public SegmentFilter(ConsistentHash hash, Set<Integer> segments) {
         this.hash = hash;
         this.segments = segments;
      }

      @Override
      public boolean accept(K key) {
         return segments.contains(hash.getSegment(key));
      }
   }

   private class SegmentChangeListener {
      private final Set<Integer> changedSegments = new ConcurrentHashSet<Integer>();

      public void changedSegments(Set<Integer> changedSegments) {
         if (log.isTraceEnabled()) {
            log.tracef("Adding changed segments %s so iteration can properly suspect them", changedSegments);
         }
         for (Integer segment : changedSegments) {
            changedSegments.add(segment);
         }
      }
   }
}
