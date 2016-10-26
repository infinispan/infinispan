package org.infinispan.query.dsl.embedded.impl;

import java.util.Map;

import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.filter.FilterIndexingServiceProvider;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.impl.FilterResultImpl;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@MetaInfServices(FilterIndexingServiceProvider.class)
@SuppressWarnings("unused")
public class JPAFilterIndexingServiceProvider extends BaseJPAFilterIndexingServiceProvider {

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == JPACacheEventFilterConverter.class;
   }

   protected Matcher getMatcher(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((JPACacheEventFilterConverter) indexedFilter).filterAndConverter.getMatcher();
   }

   protected String getQueryString(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((JPACacheEventFilterConverter) indexedFilter).filterAndConverter.getQueryString();
   }

   protected Map<String, Object> getNamedParameters(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((JPACacheEventFilterConverter) indexedFilter).filterAndConverter.getNamedParameters();
   }

   @Override
   protected boolean isDelta(IndexedFilter<?, ?, ?> indexedFilter) {
      return false;
   }

   protected void matchEvent(CacheEntryEvent event, Matcher matcher) {
      if (event.getValue() != null) {
         matcher.match(event, event.getType(), event.getValue());
      }
   }

   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      return new FilterResultImpl(instance, projection, sortProjection);
   }
}
