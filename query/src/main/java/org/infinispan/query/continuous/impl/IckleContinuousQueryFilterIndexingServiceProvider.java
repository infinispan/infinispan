package org.infinispan.query.continuous.impl;

import java.util.Map;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.notifications.cachelistener.EventWrapper;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.dsl.embedded.impl.BaseIckleFilterIndexingServiceProvider;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public class IckleContinuousQueryFilterIndexingServiceProvider extends BaseIckleFilterIndexingServiceProvider {

   private final Object joiningEvent;
   private final Object updatedEvent;
   private final Object leavingEvent;

   public IckleContinuousQueryFilterIndexingServiceProvider() {
      this(ContinuousQueryResult.ResultType.JOINING, ContinuousQueryResult.ResultType.UPDATED, ContinuousQueryResult.ResultType.LEAVING);
   }

   protected IckleContinuousQueryFilterIndexingServiceProvider(Object joiningEvent, Object updatedEvent, Object leavingEvent) {
      this.joiningEvent = joiningEvent;
      this.updatedEvent = updatedEvent;
      this.leavingEvent = leavingEvent;
   }

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == IckleContinuousQueryCacheEventFilterConverter.class;
   }

   @Override
   protected Matcher getMatcher(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((IckleContinuousQueryCacheEventFilterConverter) indexedFilter).getMatcher();
   }

   @Override
   protected String getQueryString(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((IckleContinuousQueryCacheEventFilterConverter) indexedFilter).getQueryString();
   }

   @Override
   protected Map<String, Object> getNamedParameters(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((IckleContinuousQueryCacheEventFilterConverter) indexedFilter).getNamedParameters();
   }

   @Override
   protected boolean isDelta(IndexedFilter<?, ?, ?> indexedFilter) {
      return true;
   }

   @Override
   protected <K, V> void matchEvent(EventWrapper<K, V, CacheEntryEvent<K, V>> eventWrapper, Matcher matcher) {
      CacheEntryEvent<?, ?> event = eventWrapper.getEvent();
      Object oldValue = event.getType() == Event.Type.CACHE_ENTRY_REMOVED ? ((CacheEntryRemovedEvent) event).getOldValue() : null;
      if (event.getType() == Event.Type.CACHE_ENTRY_MODIFIED) {
         oldValue = ((EventImpl) event).getOldValue();
      }
      Object newValue = event.getValue();

      if (event.getType() == Event.Type.CACHE_ENTRY_EXPIRED) {
         oldValue = newValue;   // expired events have the expired value as newValue
         newValue = null;
      }

      if (oldValue != null || newValue != null) {
         if (oldValue != null && oldValue.getClass() == WrappedByteArray.class) {
            oldValue = ((WrappedByteArray) oldValue).getBytes();
         }
         if (newValue != null && newValue.getClass() == WrappedByteArray.class) {
            newValue = ((WrappedByteArray) newValue).getBytes();
         }
         matcher.matchDelta(eventWrapper, event.getType(), oldValue, newValue, joiningEvent, updatedEvent, leavingEvent);
      }
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      return new ContinuousQueryResult<>((ContinuousQueryResult.ResultType) eventType, instance, projection);
   }
}
