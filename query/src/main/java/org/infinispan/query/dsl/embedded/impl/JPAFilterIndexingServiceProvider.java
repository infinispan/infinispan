package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.notifications.cachelistener.CacheEntryListenerInvocation;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.annotation.*;
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
import org.infinispan.objectfilter.impl.FilterResultImpl;
import org.kohsuke.MetaInfServices;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@MetaInfServices
public class JPAFilterIndexingServiceProvider implements FilterIndexingServiceProvider {

   private final ConcurrentMap<Matcher, FilteringListenerInvocation<?, ?>> filteringInvocations = new ConcurrentHashMap<Matcher, FilteringListenerInvocation<?, ?>>(2);

   private CacheNotifierImpl cacheNotifier;

   private ClusteringDependentLogic clusteringDependentLogic;

   @Inject
   protected void injectDependencies(CacheNotifier cacheNotifier, ClusteringDependentLogic clusteringDependentLogic) {
      this.cacheNotifier = (CacheNotifierImpl) cacheNotifier;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryActivated.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryCreated.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryInvalidated.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryLoaded.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryModified.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryPassivated.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryRemoved.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryVisited.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntriesEvicted.class).removeAll(filteringInvocations.values());
      cacheNotifier.getListenerCollectionForAnnotation(CacheEntryEvicted.class).removeAll(filteringInvocations.values());
      filteringInvocations.clear();
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == JPACacheEventFilterConverter.class;
   }

   @Override
   public <K, V> DelegatingCacheEntryListenerInvocation<K, V> interceptListenerInvocation(CacheEntryListenerInvocation<K, V> invocation) {
      return new DelegatingCacheEntryListenerInvocationImpl<K, V>(invocation);
   }

   @Override
   public <K, V> void registerListenerInvocations(boolean isClustered, boolean isPrimaryOnly, boolean filterAndConvert,
                                                  IndexedFilter<?, ?, ?> indexedFilter,
                                                  Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners) {
      JPAFilterAndConverter filter = ((JPACacheEventFilterConverter) indexedFilter).filterAndConverter;
      Matcher matcher = filter.getMatcher();
      addFilteringInvocationForMatcher(matcher);
      Event.Type[] eventTypes = new Event.Type[listeners.keySet().size()];
      int i = 0;
      for (Class<? extends Annotation> annotation : listeners.keySet()) {
         eventTypes[i++] = getEventType(annotation);
      }
      Callback<K, V> callback = new Callback<K, V>(matcher, isClustered, isPrimaryOnly, filterAndConvert, listeners);
      callback.subscription = matcher.registerFilter(filter.getJPAQuery(), callback, eventTypes);
   }

   /**
    * Obtains the event type that corresponds to the given event annotation.
    *
    * @param annotation a CacheEntryXXX annotation
    * @return the event type or {@code null} if the given annotation is not supported
    */
   private Event.Type getEventType(Class<? extends Annotation> annotation) {
      if (annotation == CacheEntryCreated.class) return Event.Type.CACHE_ENTRY_CREATED;
      if (annotation == CacheEntryModified.class) return Event.Type.CACHE_ENTRY_MODIFIED;
      if (annotation == CacheEntryRemoved.class) return Event.Type.CACHE_ENTRY_REMOVED;
      if (annotation == CacheEntryActivated.class) return Event.Type.CACHE_ENTRY_ACTIVATED;
      if (annotation == CacheEntryInvalidated.class) return Event.Type.CACHE_ENTRY_INVALIDATED;
      if (annotation == CacheEntryLoaded.class) return Event.Type.CACHE_ENTRY_LOADED;
      if (annotation == CacheEntryPassivated.class) return Event.Type.CACHE_ENTRY_PASSIVATED;
      if (annotation == CacheEntryVisited.class) return Event.Type.CACHE_ENTRY_VISITED;
      if (annotation == CacheEntriesEvicted.class) return Event.Type.CACHE_ENTRY_EVICTED;
      if (annotation == CacheEntryEvicted.class) return Event.Type.CACHE_ENTRY_EVICTED;
      return null;
   }

   private void addFilteringInvocationForMatcher(Matcher matcher) {
      if (!filteringInvocations.containsKey(matcher)) {
         FilteringListenerInvocation filteringInvocation = new FilteringListenerInvocation(matcher);
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
            cacheNotifier.getListenerCollectionForAnnotation(CacheEntryEvicted.class).add(filteringInvocation);
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

      private Matcher matcher;
      protected FilterSubscription subscription;

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
         evicted_invocations = concatArrays(makeArray(listeners, CacheEntriesEvicted.class), makeArray(listeners, CacheEntryEvicted.class));
      }

      private DelegatingCacheEntryListenerInvocation<K, V>[] makeArray(Map<Class<? extends Annotation>, List<DelegatingCacheEntryListenerInvocation<K, V>>> listeners, Class<? extends Annotation> eventType) {
         List<DelegatingCacheEntryListenerInvocation<K, V>> invocations = listeners.get(eventType);
         if (invocations == null) {
            return null;
         }
         DelegatingCacheEntryListenerInvocation[] invocationsArray = invocations.toArray(new DelegatingCacheEntryListenerInvocation[invocations.size()]);
         for (DelegatingCacheEntryListenerInvocation di : invocationsArray) {
            ((DelegatingCacheEntryListenerInvocationImpl) di).callback = this;
         }
         return invocationsArray;
      }

      private <T> T[] concatArrays(T[] first, T[] second) {
         if (first == null) {
            return second;
         }
         if (second == null) {
            return first;
         }
         T[] result = Arrays.copyOf(first, first.length + second.length);
         System.arraycopy(second, 0, result, first.length, second.length);
         return result;
      }

      void unregister() {
         if (subscription != null) {
            // unregister only once
            matcher.unregisterFilter(subscription);
            subscription = null;
         }
      }

      @Override
      public void onFilterResult(Object userContext, Object instance, Object eventType, Object[] projection, Comparable[] sortProjection) {
         DelegatingCacheEntryListenerInvocation<K, V>[] invocations;
         switch ((Event.Type) eventType) {
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
            default:
               return;
         }

         if (invocations != null) {
            CacheEntryEvent<K, V> event = (CacheEntryEvent<K, V>) userContext;
            if (event.isPre() && isClustered || isPrimaryOnly && !clusteringDependentLogic.localNodeIsPrimaryOwner(event.getKey())) {
               return;
            }
            if (filterAndConvert && event instanceof EventImpl) {
               EventImpl<K, V> eventImpl = (EventImpl<K, V>) event;
               EventImpl<K, V> clone = eventImpl.clone();
               clone.setValue((V) makeFilterResult(projection == null ? instance : null, projection, sortProjection));
               event = clone;
            }
            for (DelegatingCacheEntryListenerInvocation<K, V> invocation : invocations) {
               invocation.invokeNoChecks(event, false, filterAndConvert);
            }
         }
      }
   }

   protected Object makeFilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
      return new FilterResultImpl(instance, projection, sortProjection);
   }

   private class DelegatingCacheEntryListenerInvocationImpl<K, V> extends DelegatingCacheEntryListenerInvocation<K, V> {

      protected Callback<K, V> callback;

      public DelegatingCacheEntryListenerInvocationImpl(CacheEntryListenerInvocation<K, V> invocation) {
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

      private FilteringListenerInvocation(Matcher matcher) {
         this.matcher = matcher;
      }

      @Override
      public Object getTarget() {
         return JPAFilterIndexingServiceProvider.this;
      }

      @Override
      public void invoke(Event<K, V> event) {
      }

      @Override
      public void invoke(CacheEntryEvent<K, V> event, boolean isLocalNodePrimaryOwner) {
         if (event.getValue() != null) {
            matcher.match(event, event.getValue(), event.getType());
         }
      }

      @Override
      public void invokeNoChecks(CacheEntryEvent<K, V> event, boolean skipQueue, boolean skipConverter) {
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
   }
}
