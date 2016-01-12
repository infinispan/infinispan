package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.Version;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.filter.Filters;
import org.infinispan.client.hotrod.impl.operations.*;
import org.infinispan.client.hotrod.impl.iteration.RemoteCloseableIterator;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.query.dsl.Query;

import static org.infinispan.client.hotrod.filter.Filters.makeFactoryParams;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RemoteCacheImpl<K, V> extends RemoteCacheSupport<K, V> {

   private static final Log log = LogFactory.getLog(RemoteCacheImpl.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private Marshaller marshaller;
   private final String name;
   private final RemoteCacheManager remoteCacheManager;
   private volatile ExecutorService executorService;
   protected OperationsFactory operationsFactory;
   private int estimateKeySize;
   private int estimateValueSize;
   private volatile boolean hasCompatibility;

   public RemoteCacheImpl(RemoteCacheManager rcm, String name) {
      if (trace) {
         log.tracef("Creating remote cache: %s", name);
      }
      this.name = name;
      this.remoteCacheManager = rcm;
   }

   public void init(Marshaller marshaller, ExecutorService executorService, OperationsFactory operationsFactory, int estimateKeySize, int estimateValueSize) {
      this.marshaller = marshaller;
      this.executorService = executorService;
      this.operationsFactory = operationsFactory;
      this.estimateKeySize = estimateKeySize;
      this.estimateValueSize = estimateValueSize;
   }

   public OperationsFactory getOperationsFactory() {
      return operationsFactory;
   }

   @Override
   public RemoteCacheManager getRemoteCacheManager() {
      return remoteCacheManager;
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      assertRemoteCacheManagerIsStarted();
      RemoveIfUnmodifiedOperation<V> op = operationsFactory.newRemoveIfUnmodifiedOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true), version);
      VersionedOperationResponse<V> response = op.execute();
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> removeWithVersionAsync(final K key, final long version) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               boolean removed = removeWithVersion(key, version);
               try {
                  result.notifyDone(removed);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return removed;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, TimeUnit.SECONDS, maxIdleTimeSeconds, TimeUnit.SECONDS);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceIfUnmodifiedOperation op = operationsFactory.newReplaceIfUnmodifiedOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true), obj2bytes(newValue, false), lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version);
      VersionedOperationResponse response = op.execute();
      return response.getCode().isUpdated();
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(final K key, final V newValue, final long version, final int lifespanSeconds, final int maxIdleSeconds) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Boolean> result = new NotifyingFutureImpl<Boolean>();
      Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            try {
               boolean removed = replaceWithVersion(key, newValue, version, lifespanSeconds, maxIdleSeconds);
               try {
                  result.notifyDone(removed);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return removed;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      assertRemoteCacheManagerIsStarted();
      if (segments != null && segments.isEmpty()) {
         return new CloseableIterator<Entry<Object, Object>>() {
            @Override
            public void close() {
            }

            @Override
            public boolean hasNext() {
               return false;
            }

            @Override
            public Entry<Object, Object> next() {
               throw new NoSuchElementException();
            }
         };
      }
      byte[][] params = marshallParams(filterConverterParams);
      RemoteCloseableIterator remoteCloseableIterator = new RemoteCloseableIterator(operationsFactory,
              filterConverterFactory, params, segments, batchSize, false);
      remoteCloseableIterator.start();
      return remoteCloseableIterator;
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntriesByQuery(Query filterQuery, Set<Integer> segments, int batchSize) {
      Object[] factoryParams = makeFactoryParams(filterQuery);
      return retrieveEntries(Filters.ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME, factoryParams, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, MetadataValue<Object>>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      RemoteCloseableIterator remoteCloseableIterator = new RemoteCloseableIterator(operationsFactory, batchSize, segments, true);
      remoteCloseableIterator.start();
      return remoteCloseableIterator;
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithVersionOperation<V> op = operationsFactory.newGetWithVersionOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true));
      return op.execute();
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      assertRemoteCacheManagerIsStarted();
      GetWithMetadataOperation<V> op = operationsFactory.newGetWithMetadataOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true));
      return op.execute();
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to putAll entries (%s) lifespan:%d (%s), maxIdle:%d (%s)", map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      }
      Map<byte[], byte[]> byteMap = new HashMap<>();
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         byteMap.put(obj2bytes(entry.getKey(),  true), obj2bytes(entry.getValue(), false));
      }
      PutAllOperation op = operationsFactory.newPutAllOperation(byteMap, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      op.execute();
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<Void>();
      Future<Void> future = executorService.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               putAll(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
               try {
                  result.notifyDone(null);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return null;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;

   }

   @Override
   public int size() {
      assertRemoteCacheManagerIsStarted();
      SizeOperation op = operationsFactory.newSizeOperation();
      return op.execute();
   }

   @Override
   public boolean isEmpty() {
      return size() == 0;
   }

   @Override
   public ServerStatistics stats() {
      assertRemoteCacheManagerIsStarted();
      StatsOperation op = operationsFactory.newStatsOperation();
      Map<String, String> statsMap = op.execute();
      ServerStatisticsImpl stats = new ServerStatisticsImpl();
      for (Map.Entry<String, String> entry : statsMap.entrySet()) {
         stats.addStats(entry.getKey(), entry.getValue());
      }
      return stats;
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to add (K,V): (%s, %s) lifespan:%d, maxIdle:%d", key, value, lifespan, maxIdleTime);
      }
      PutOperation<V> op = operationsFactory.newPutKeyValueOperation(compatKeyIfNeeded(key),
         obj2bytes(key, true), obj2bytes(value, false), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return op.execute();
   }

   private K compatKeyIfNeeded(Object key) {
      return hasCompatibility ? (K) key : null;
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      PutIfAbsentOperation<V> op = operationsFactory.newPutIfAbsentOperation(compatKeyIfNeeded(key),
         obj2bytes(key, true), obj2bytes(value, false), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return op.execute();
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      assertRemoteCacheManagerIsStarted();
      ReplaceOperation<V> op = operationsFactory.newReplaceOperation(compatKeyIfNeeded(key),
         obj2bytes(key, true), obj2bytes(value, false), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return op.execute();
   }

   @Override
   public NotifyingFuture<V> putAsync(final K key, final V value, final long lifespan, final TimeUnit lifespanUnit, final long maxIdle, final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new WithFlagsCallable(operationsFactory.flags()) {
         @Override
         public V call() throws Exception {
            try {
               setFlagsIfPresent();
               V prevValue =
                     RemoteCacheImpl.this.put(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
               try {
                  result.notifyDone(prevValue);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return prevValue;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<Void> result = new NotifyingFutureImpl<Void>();
      Future<Void> future = executorService.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            try {
               clear();
               try {
                  result.notifyDone(null);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return null;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdle,final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new WithFlagsCallable(operationsFactory.flags()) {
         @Override
         public V call() throws Exception {
            try {
               setFlagsIfPresent();
               V prevValue = putIfAbsent(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
               try {
                  result.notifyDone(prevValue);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return prevValue;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public NotifyingFuture<V> removeAsync(final Object key) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new WithFlagsCallable(operationsFactory.flags()) {
         @Override
         public V call() throws Exception {
            try {
               setFlagsIfPresent();
               V toReturn = remove(key);
               try {
                  result.notifyDone(toReturn);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return toReturn;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public NotifyingFuture<V> replaceAsync(final K key,final V value,final long lifespan,final TimeUnit lifespanUnit,final long maxIdle,final TimeUnit maxIdleUnit) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new WithFlagsCallable(operationsFactory.flags()) {
         @Override
         public V call() throws Exception {
            try {
               setFlagsIfPresent();
               V old = replace(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
               try {
                  result.notifyDone(old);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return old;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   @Override
   public boolean containsKey(Object key) {
      assertRemoteCacheManagerIsStarted();
      ContainsKeyOperation op = operationsFactory.newContainsKeyOperation(
         compatKeyIfNeeded(key), obj2bytes(key, true));
      return op.execute();
   }

   @Override
   public V get(Object key) {
      assertRemoteCacheManagerIsStarted();
      byte[] keyBytes = obj2bytes(key, true);
      GetOperation<V> gco = operationsFactory.newGetKeyOperation(compatKeyIfNeeded(key), keyBytes);
      V result = gco.execute();
      if (trace) {
         log.tracef("For key(%s) returning %s", key, result);
      }
      return result;
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      assertRemoteCacheManagerIsStarted();
      if (trace) {
         log.tracef("About to getAll entries (%s)", keys);
      }
      Set<byte[]> byteKeys = new HashSet<>(keys.size());
      for (K key : keys) {
         byteKeys.add(obj2bytes(key, true));
      }
      GetAllOperation<K, V> op = operationsFactory.newGetAllOperation(byteKeys);
      Map<K, V> result = op.execute();
      return Collections.unmodifiableMap(result);
   }

   @Override
   public Map<K, V> getBulk() {
      return getBulk(0);
   }

   @Override
   public Map<K, V> getBulk(int size) {
      assertRemoteCacheManagerIsStarted();
      BulkGetOperation<K, V> op = operationsFactory.newBulkGetOperation(size);
      Map<K, V> result = op.execute();
      return Collections.unmodifiableMap(result);
   }

   @Override
   public V remove(Object key) {
      assertRemoteCacheManagerIsStarted();
      RemoveOperation<V> removeOperation = operationsFactory.newRemoveOperation(compatKeyIfNeeded(key), obj2bytes(key, true));
      // TODO: It sucks that you need the prev value to see if it works...
      // We need to find a better API for RemoteCache...
      return removeOperation.execute();
   }

   @Override
   public void clear() {
      assertRemoteCacheManagerIsStarted();
      ClearOperation op = operationsFactory.newClearOperation() ;
      op.execute();
   }

   @Override
   public void start() {
      if (log.isDebugEnabled()) {
         log.debugf("Start called, nothing to do here(%s)", getName());
      }
   }

   @Override
   public void stop() {
      if (log.isDebugEnabled()) {
         log.debugf("Stop called, nothing to do here(%s)", getName());
      }
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String getVersion() {
      return RemoteCacheImpl.class.getPackage().getImplementationVersion();
   }

   @Override
   public String getProtocolVersion() {
      return Version.getProtocolVersion();
   }

   @Override
   public void addClientListener(Object listener) {
      assertRemoteCacheManagerIsStarted();
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(listener);
      op.execute();
   }

   @Override
   public void addClientListener(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      assertRemoteCacheManagerIsStarted();
      byte[][] marshalledFilterParams = marshallParams(filterFactoryParams);
      byte[][] marshalledConverterParams = marshallParams(converterFactoryParams);
      AddClientListenerOperation op = operationsFactory.newAddClientListenerOperation(
            listener, marshalledFilterParams, marshalledConverterParams);
      op.execute();
   }

   private byte[][] marshallParams(Object[] params) {
      if (params == null)
         return new byte[0][];

      byte[][] marshalledParams = new byte[params.length][];
      for (int i = 0; i < marshalledParams.length; i++) {
         byte[] bytes = obj2bytes(params[i], true);// should be small
         marshalledParams[i] = bytes;
      }

      return marshalledParams;
   }

   @Override
   public void removeClientListener(Object listener) {
      assertRemoteCacheManagerIsStarted();
      RemoveClientListenerOperation op = operationsFactory.newRemoveClientListenerOperation(listener);
      op.execute();
   }

   @Override
   public Set<Object> getListeners() {
      ClientListenerNotifier listenerNotifier = operationsFactory.getListenerNotifier();
      return listenerNotifier.getListeners(operationsFactory.getCacheName());
   }

   @Override
   public RemoteCache<K, V> withFlags(Flag... flags) {
      operationsFactory.setFlags(flags);
      return this;
   }

   @Override
   public NotifyingFuture<V> getAsync(final K key) {
      assertRemoteCacheManagerIsStarted();
      final NotifyingFutureImpl<V> result = new NotifyingFutureImpl<V>();
      Future<V> future = executorService.submit(new Callable<V>() {
         @Override
         public V call() throws Exception {
            try {
               V toReturn = get(key);
               try {
                  result.notifyDone(toReturn);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               return toReturn;
            } catch (Exception e) {
               try {
                  result.notifyException(e);
               } catch (Throwable t) {
                  log.trace("Error when notifying", t);
               }
               throw e;
            }
         }
      });
      result.setFuture(future);
      return result;
   }

   public PingOperation.PingResult ping() {
      return operationsFactory.newFaultTolerantPingOperation().execute();
   }

   private byte[] obj2bytes(Object o, boolean isKey) {
      try {
         return marshaller.objectToByteBuffer(o, isKey ? estimateKeySize : estimateValueSize);
      } catch (IOException ioe) {
         throw new HotRodClientException(
               "Unable to marshall object of type [" + o.getClass().getName() + "]", ioe);
      } catch (InterruptedException ie) {
         Thread.currentThread().interrupt();
         return null;
      }
   }

   private void assertRemoteCacheManagerIsStarted() {
      if (!remoteCacheManager.isStarted()) {
         String message = "Cannot perform operations on a cache associated with an unstarted RemoteCacheManager. Use RemoteCacheManager.start before using the remote cache.";
         if (log.isInfoEnabled()) {
            log.unstartedRemoteCacheManager();
         }
         throw new RemoteCacheManagerNotStartedException(message);
      }
   }

   @Override
   protected void set(K key, V value) {
      // no need to optimize the put operation: all invocations are already non-return by default,
      // see org.infinispan.client.hotrod.Flag.FORCE_RETURN_VALUE
      // Warning: never invoke put(K,V) in this scope or we'll get a stackoverflow.
      put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public Set<K> keySet() {
	   assertRemoteCacheManagerIsStarted();
	   // Use default scope
	   BulkGetKeysOperation<K> op = operationsFactory.newBulkGetKeysOperation(0);
      return Collections.unmodifiableSet(op.execute());
   }

	@Override
	public <T> T execute(String taskName, Map<String, ?> params) {
		assertRemoteCacheManagerIsStarted();
		Map<String, byte[]> marshalledParams = new HashMap<>();
		if (params != null) {
   		for(java.util.Map.Entry<String, ?> entry : params.entrySet()) {
   			marshalledParams.put(entry.getKey(), obj2bytes(entry.getValue(), false));
   		}
		}
		ExecuteOperation<T> op = operationsFactory.newExecuteOperation(taskName, marshalledParams);
		return op.execute();
	}

   @Override
   public CacheTopologyInfo getCacheTopologyInfo() {
      return operationsFactory.getCacheTopologyInfo();
   }

   public PingOperation.PingResult resolveCompatibility() {
      if (remoteCacheManager.isStarted()) {
         PingOperation.PingResult result = ping();
         hasCompatibility = result.hasCompatibility();
         return result;
      }

      return PingOperation.PingResult.FAIL;
   }

   private abstract class WithFlagsCallable implements Callable<V> {
      final int intFlags;

      protected WithFlagsCallable(int intFlags) {
         this.intFlags = intFlags;
      }

      void setFlagsIfPresent() {
         if (intFlags != 0)
            operationsFactory.setFlags(intFlags);
      }
   }

}
