package org.infinispan.query.remote.impl.filter;

import java.util.Map;

import org.infinispan.filter.NamedFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@NamedFactory(name = IckleCacheEventFilterConverterFactory.FACTORY_NAME)
@MetaInfServices(CacheEventFilterConverterFactory.class)
public final class IckleCacheEventFilterConverterFactory
      extends AbstractIckleFilterConverterFactory<CacheEventFilterConverter>
      implements CacheEventFilterConverterFactory {

   public static final String FACTORY_NAME = "query-dsl-filter-converter-factory";

   @Override
   protected CacheEventFilterConverter getFilterConverter(String queryString, Map<String, Object> namedParams) {
      return new IckleProtobufCacheEventFilterConverter(new IckleProtobufFilterAndConverter(queryString, namedParams));
   }
}
