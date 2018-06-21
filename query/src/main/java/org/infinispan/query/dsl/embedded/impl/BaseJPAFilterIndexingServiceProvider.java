package org.infinispan.query.dsl.embedded.impl;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheEntryListenerInvocation;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.EventWrapper;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.DelegatingCacheEntryListenerInvocation;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.Matcher;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public abstract class BaseJPAFilterIndexingServiceProvider implements FilterIndexingServiceProvider {

   private final ConcurrentMap<Matcher, FilteringListenerInvocation<?, ?>> filteringInvocations = new ConcurrentHashMap<>(4);

   private CacheNotifierImpl cacheNotifier;

   private ClusteringDependentLogic clusteringDependentLogic;

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(CacheNotifier cacheNotifier, ClusteringDependentLogic clusteringDependentLogic) {
      this.cacheNotifier = (CacheNotifierImpl) cacheNotifier;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
      Collection<FilteringListenerInvocation<?, ?>> invocations = filteringInvocations.values();
      if (cacheNotifier != null) {
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryActivated.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryCreated.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryInvalidated.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryLoaded.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryModified.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryPassivated.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryRemoved.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryVisited.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntriesEvicted.class).removeAll(invocations);
         cacheNotifier.getListenerCollectionForAnnotation(CacheEntryExpired.class).removeAll(invocations);
      }
      filteringInvocations.clear();
   }

   @Override
   public <K, V> DelegatingCacheEntryListenerInvocation<K, V> interceptListenerInvocation(CacheEntryListenerInvocation<K, V> invocation) {
      return new DelegatingCacheEntryListenerInvocationImpl<>(invocation);
   }

   @Override
   public <K, V> void registerListenerInvocations(boolean isClustered, boolean isPrimaryOnly, boolean filterAndConvert,
                                                  IndexedFilter<?, ?, ?> indexedFilter,
                                                  Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners,
                                                  DataConversion keyDataConversion,
                                                  DataConversion valueDataConversion) {
      final Matcher matcher = getMatcher(indexedFilter);
      final String queryString = getQueryString(indexedFilter);
      final Map<String, Object> namedParameters = getNamedParameters(indexedFilter);
      final boolean isDeltaFilter = isDelta(indexedFilter);

      addFilteringInvocationForMatcher(matcher, keyDataConversion, valueDataConversion);
      Event.Type[] eventTypes = new Event.Type[listeners.keySet().size()];
      int i = 0;
      for (Class<? extends Annotation> annotation : listeners.keySet()) {
         eventTypes[i++] = getEventTypeFromAnnotation(annotation);
      }
      Callback<K, V> callback = new Callback<>(matcher, isClustered, isPrimaryOnly, filterAndConvert, listeners);
      callback.subscription = matcher.registerFilter(queryString, namedParameters, callback, isDeltaFilter, eventTypes);
   }

   /**
    * Obtains the event type that corresponds to the given event annotation.
    *
    * @param annotation a CacheEntryXXX annotation
    * @return the event type or {@code null} if the given annotation is not supported
    */
   private Event.Type getEventTypeFromAnnotation(Class<? extends Annotation> annotation) {
      if (annotation == CacheEntryCreated.class) return Event.Type.CACHE_ENTRY_CREATED;
      if (annotation == CacheEntryModified.class) return Event.Type.CACHE_ENTRY_MODIFIED;
      if (annotation == CacheEntryRemoved.class) return Event.Type.CACHE_ENTRY_REMOVED;
      if (annotation == CacheEntryActivated.class) return Event.Type.CACHE_ENTRY_ACTIVATED;
      if (annotation == CacheEntryInvalidated.class) return Event.Type.CACHE_ENTRY_INVALIDATED;
      if (annotation == CacheEntryLoaded.class) return Event.Type.CACHE_ENTRY_LOADED;
      if (annotation == CacheEntryPassivated.class) return Event.Type.CACHE_ENTRY_PASSIVATED;
      if (annotation == CacheEntryVisited.class) return Event.Type.CACHE_ENTRY_VISITED;
      if (annotation == CacheEntriesEvicted.class) return Event.Type.CACHE_ENTRY_EVICTED;
      if (annotation == CacheEntryExpired.class) return Event.Type.CACHE_ENTRY_EXPIRED;
      return null;
   }

   private void addFilteringInvocationForMatcher(Matcher matcher, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      if (!filteringInvocations.containsKey(matcher)) {
         FilteringListenerInvocation filteringInvocation = new FilteringListenerInvocation(matcher, keyDataConversion, valueDataConversion);
         if (filteringInvocations.putIfAbsent(matcher, filteringInvocation) == null) {
            // todo these are added but never removed until stop is called
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryActivated.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryCreated.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryInvalidated.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryLoaded.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryModified.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryPassivated.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryRemoved.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryVisited.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntriesEvicted.class).add(filteringInvocation);
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryExpired.class).add(filteringInvocation);
         }
      }
   }

   private class Callback<K, V> implements FilterCallback {

      private final boolean isClustered;
      private final boolean isPrimaryOnly;
      private final boolean filterAndConvert;

      private final DelegatingCacheEntryListenerInvocation<K, V>[] activated_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] created_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] invalidated_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] loaded_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] modified_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] passivated_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] removed_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] visited_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] evicted_invocations;
      private final DelegatingCacheEntryListenerInvocation<K, V>[] expired_invocations;

      private final Matcher matcher;
      protected volatile FilterSubscription subscription;

      Callback(Matcher matcher, boolean isClustered, boolean isPrimaryOnly, boolean filterAndConvert, Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners) {
         this.matcher = matcher;
         this.isClustered = isClustered;
         this.isPrimaryOnly = isPrimaryOnly;
         this.filterAndConvert = filterAndConvert;
         activated_invocations = makeArray(listeners, CacheEntryActivated.class);
         created_invocations = makeArray(listeners, CacheEntryCreated.class);
         invalidated_invocations = makeArray(listeners, CacheEntryInvalidated.class);
         loaded_invocations = makeArray(listeners, CacheEntryLoaded.class);
         modified_invocations = makeArray(listeners, CacheEntryModified.class);
         passivated_invocations = makeArray(listeners, CacheEntryPassivated.class);
         removed_invocations = makeArray(listeners, CacheEntryRemoved.class);
         visited_invocations = makeArray(listeners, CacheEntryVisited.class);
         evicted_invocations = makeArray(listeners, CacheEntriesEvicted.class);
         expired_invocations = makeArray(listeners, CacheEntryExpired.class);
      }

      private DelegatingCacheEntryListenerInvocation<K, V>[] makeArray(Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners, Class<? extends Annotation> eventType) {
         List<DelegatingCacheEntryListenerInvocation<K, V>> invocations = listeners.get(eventType);
         if (invocations == null) {
            return null;
         }
         DelegatingCacheEntryListenerInvocation<K, V>[] invocationsArray = invocations.toArray(new DelegatingCacheEntryListenerInvocation[invocations.size()]);
         for (DelegatingCacheEntryListenerInvocation di : invocationsArray) {
            ((DelegatingCacheEntryListenerInvocationImpl) di).callback = this;
         }
         return invocationsArray;
      }

      void unregister() {
         FilterSubscription s = subscription;
         if (s != null) {
            // unregister only once
            matcher.unregisterFilter(s);
            subscription = null;
         }
      }

      @Override
      public void onFilterResult(Object userContext, Object eventType, Object instance, Object[] projection, Comparable[] sortProjection) {
         EventWrapper eventWrapper = (EventWrapper) userContext;
         CacheEntryEvent<K, V> event = (CacheEntryEvent<K, V>) eventWrapper.getEvent();
         if (event.isPre() && isClustered || isPrimaryOnly &&
               !clusteringDependentLogic.getCacheTopology().getDistribution(eventWrapper.getKey()).isPrimary()) {
            return;
         }

         DelegatingCacheEntryListenerInvocation<K, V>[] invocations;
         switch (event.getType()) {
            case CACHE_ENTRY_ACTIVATED:
               invocations = activated_invocations;
               break;
            case CACHE_ENTRY_CREATED:
               invocations = created_invocations;
               break;
            case CACHE_ENTRY_INVALIDATED:
               invocations = invalidated_invocations;
               break;
            case CACHE_ENTRY_LOADED:
               invocations = loaded_invocations;
               break;
            case CACHE_ENTRY_MODIFIED:
               invocations = modified_invocations;
               break;
            case CACHE_ENTRY_PASSIVATED:
               invocations = passivated_invocations;
               break;
            case CACHE_ENTRY_REMOVED:
               invocations = removed_invocations;
               break;
            case CACHE_ENTRY_VISITED:
               invocations = visited_invocations;
               break;
            case CACHE_ENTRY_EVICTED:
               invocations = evicted_invocations;
               break;
            case CACHE_ENTRY_EXPIRED:
               invocations = expired_invocations;
               break;
            default:
               return;
         }

         boolean conversionDone = false;
         for (DelegatingCacheEntryListenerInvocation<K, V> invocation : invocations) {
            if (invocation.getObservation().shouldInvoke(event.isPre())) {
               if (!conversionDone) {
                  if (filterAndConvert && event instanceof EventImpl) {   //todo [anistor] can it not be an EventImpl? can it not be filterAndConvert?
                     EventImpl<K, V> eventImpl = (EventImpl<K, V>) event;
                     EventImpl<K, V> clone = eventImpl.clone();
                     clone.setValue((V) makeFilterResult(userContext, eventType, event.getKey(), projection == null ? instance : null, projection, sortProjection));
                     event = clone;
                  }
                  conversionDone = true;
               }

               invocation.invokeNoChecks(new EventWrapper<>(event.getKey(), event), false, filterAndConvert);
            }
         }
      }
   }

   private class DelegatingCacheEntryListenerInvocationImpl<K, V> extends DelegatingCacheEntryListenerInvocation<K, V> {

      protected Callback<K, V> callback;

      DelegatingCacheEntryListenerInvocationImpl(CacheEntryListenerInvocation<K, V> invocation) {
         super(invocation);
      }

      @Override
      public void unregister() {
         if (callback != null) {
            callback.unregister();
         }
      }
   }

   private class FilteringListenerInvocation<K, V> implements CacheEntryListenerInvocation<K, V> {

      private final Matcher matcher;
      private final DataConversion keyDataConversion;
      private final DataConversion valueDataConversion;

      private FilteringListenerInvocation(Matcher matcher, DataConversion keyDataConversion, DataConversion valueDataConversion) {
         this.matcher = matcher;
         this.keyDataConversion = keyDataConversion;
         this.valueDataConversion = valueDataConversion;
      }

      @Override
      public Object getTarget() {
         return BaseJPAFilterIndexingServiceProvider.this;
      }

      @Override
      public void invoke(Event<K, V> event) {
      }

      @Override
      public void invoke(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean isLocalNodePrimaryOwner) {
         matchEvent(event, matcher);
      }

      @Override
      public void invokeNoChecks(EventWrapper<K, V, CacheEntryEvent<K, V>> event, boolean skipQueue, boolean skipConverter) {
      }

      @Override
      public boolean isClustered() {
         return false;
      }

      @Override
      public boolean isSync() {
         return true;
      }

      @Override
      public UUID getIdentifier() {
         return null;
      }

      @Override
      public Listener.Observation getObservation() {
         return Listener.Observation.BOTH;
      }

      @Override
      public Class<? extends Annotation> getAnnotation() {
         return null;
      }

      @Override
      public CacheEventFilter<? super K, ? super V> getFilter() {
         return null;
      }

      @Override
      public <C> CacheEventConverter<? super K, ? super V, C> getConverter() {
         return null;
      }

      @Override
      public Set<Class<? extends Annotation>> getFilterAnnotations() {
         return null;
      }

      @Override
      public DataConversion getKeyDataConversion() {
         return keyDataConversion;
      }

      @Override
      public DataConversion getValueDataConversion() {
         return valueDataConversion;
      }
   }

   protected abstract Matcher getMatcher(IndexedFilter<?, ?, ?> indexedFilter);

   protected abstract String getQueryString(IndexedFilter<?, ?, ?> indexedFilter);

   protected abstract Map<String, Object> getNamedParameters(IndexedFilter<?, ?, ?> indexedFilter);

   protected abstract boolean isDelta(IndexedFilter<?, ?, ?> indexedFilter);

   protected abstract <K, V> void matchEvent(EventWrapper<K, V, CacheEntryEvent<K, V>> eventWrapper, Matcher matcher);

   protected abstract Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection);
}
