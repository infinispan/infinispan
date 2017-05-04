package org.infinispan.query.remote.impl.filter;

import static org.infinispan.query.remote.impl.filter.IckleFilterConverterUtils.unmarshallParams;
import static org.infinispan.query.remote.impl.filter.IckleFilterConverterUtils.unmarshallQueryString;

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
@MetaInfServices
public final class IckleCacheEventFilterConverterFactory implements CacheEventFilterConverterFactory {

   public static final String FACTORY_NAME = "query-dsl-filter-converter-factory";

   @Override
   public CacheEventFilterConverter<?, ?, ?> getFilterConverter(Object[] params) {
      String queryString = unmarshallQueryString(params);
      Map<String, Object> namedParams = unmarshallParams(params);
      return new IckleProtobufCacheEventFilterConverter(new IckleProtobufFilterAndConverter(queryString, namedParams));
   }
}
