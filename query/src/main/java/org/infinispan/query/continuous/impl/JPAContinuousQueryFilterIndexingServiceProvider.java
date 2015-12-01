package org.infinispan.query.continuous.impl;

import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.dsl.embedded.impl.BaseJPAFilterIndexingServiceProvider;
import org.kohsuke.MetaInfServices;

import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public class JPAContinuousQueryFilterIndexingServiceProvider extends BaseJPAFilterIndexingServiceProvider {

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == JPAContinuousQueryCacheEventFilterConverter.class;
   }

   @Override
   protected Matcher getMatcher(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((JPAContinuousQueryCacheEventFilterConverter) indexedFilter).getMatcher();
   }

   @Override
   protected String getJPAQuery(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((JPAContinuousQueryCacheEventFilterConverter) indexedFilter).getJPAQuery();
   }

   @Override
   protected Map<String, Object> getNamedParameters(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((JPAContinuousQueryCacheEventFilterConverter) indexedFilter).getNamedParameters();
   }

   @Override
   protected boolean isDelta(IndexedFilter<?, ?, ?> indexedFilter) {
      return true;
   }

   @Override
   protected void matchEvent(CacheEntryEvent event, Matcher matcher) {
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
         matcher.matchDelta(event, event.getType(), oldValue, newValue, true, false);
      }
   }

   @Override
   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      boolean isJoining = Boolean.TRUE.equals(eventType);
      return new ContinuousQueryResult(isJoining, instance, projection);
   }
}
