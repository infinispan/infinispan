package org.infinispan.query.dsl.embedded.impl;

import java.util.Map;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.notifications.cachelistener.EventWrapper;
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
public class IckleFilterIndexingServiceProvider extends BaseIckleFilterIndexingServiceProvider {

   @Override
   public boolean supportsFilter(IndexedFilter<?, ?, ?> indexedFilter) {
      return indexedFilter.getClass() == IckleCacheEventFilterConverter.class;
   }

   protected Matcher getMatcher(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((IckleCacheEventFilterConverter) indexedFilter).filterAndConverter.getMatcher();
   }

   protected String getQueryString(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((IckleCacheEventFilterConverter) indexedFilter).filterAndConverter.getQueryString();
   }

   protected Map<String, Object> getNamedParameters(IndexedFilter<?, ?, ?> indexedFilter) {
      return ((IckleCacheEventFilterConverter) indexedFilter).filterAndConverter.getNamedParameters();
   }

   @Override
   protected boolean isDelta(IndexedFilter<?, ?, ?> indexedFilter) {
      return false;
   }

   @Override
   protected <K, V> void matchEvent(EventWrapper<K, V, CacheEntryEvent<K, V>> eventWrapper, Matcher matcher) {
      CacheEntryEvent<?, ?> event = eventWrapper.getEvent();
      Object instance = event.getValue();
      if (instance != null) {
         if (instance.getClass() == WrappedByteArray.class) {
            instance = ((WrappedByteArray) instance).getBytes();
         }
         matcher.match(eventWrapper, event.getType(), instance);
      }
   }

   protected Object makeFilterResult(Object userContext, Object eventType, Object key, Object instance, Object[] projection, Comparable[] sortProjection) {
      return new FilterResultImpl(instance, projection, sortProjection);
   }
}
