package org.infinispan.query.remote.impl.filter;

import java.util.Map;

import org.infinispan.filter.NamedFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@NamedFactory(name = IckleContinuousQueryProtobufCacheEventFilterConverterFactory.FACTORY_NAME)
@MetaInfServices(CacheEventFilterConverterFactory.class)
public final class IckleContinuousQueryProtobufCacheEventFilterConverterFactory
      extends AbstractIckleFilterConverterFactory<CacheEventFilterConverter>
      implements CacheEventFilterConverterFactory {

   public static final String FACTORY_NAME = "continuous-query-filter-converter-factory";

   @Override
   protected CacheEventFilterConverter getFilterConverter(String queryString, Map<String, Object> namedParams) {
      return new IckleContinuousQueryProtobufCacheEventFilterConverter(queryString, namedParams, ProtobufMatcher.class);
   }
}
