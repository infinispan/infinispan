package org.infinispan.query.remote.impl.filter;

import static org.infinispan.query.remote.impl.filter.IckleFilterConverterUtils.unmarshallParams;
import static org.infinispan.query.remote.impl.filter.IckleFilterConverterUtils.unmarshallQueryString;

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
@MetaInfServices
public final class IckleContinuousQueryProtobufCacheEventFilterConverterFactory implements CacheEventFilterConverterFactory {

   public static final String FACTORY_NAME = "continuous-query-filter-converter-factory";

   @Override
   public CacheEventFilterConverter<?, ?, ?> getFilterConverter(Object[] params) {
      String queryString = unmarshallQueryString(params);
      Map<String, Object> namedParams = unmarshallParams(params);
      return new IckleContinuousQueryProtobufCacheEventFilterConverter(queryString, namedParams, ProtobufMatcher.class);
   }
}
