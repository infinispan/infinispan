package org.infinispan.query.remote.impl.filter;

import org.infinispan.filter.NamedFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.kohsuke.MetaInfServices;

import java.util.Map;

import static org.infinispan.query.remote.impl.filter.JPAFilterConverterUtils.unmarshallJPQL;
import static org.infinispan.query.remote.impl.filter.JPAFilterConverterUtils.unmarshallParams;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@NamedFactory(name = JPACacheEventFilterConverterFactory.FACTORY_NAME)
@MetaInfServices
public final class JPACacheEventFilterConverterFactory implements CacheEventFilterConverterFactory {

   public static final String FACTORY_NAME = "query-dsl-filter-converter-factory";

   @Override
   public CacheEventFilterConverter<?, ?, ?> getFilterConverter(Object[] params) {
      String jpql = unmarshallJPQL(params);
      Map<String, Object> namedParams = unmarshallParams(params);
      //todo [anistor] test this in compat mode too!
      return new JPAProtobufCacheEventFilterConverter(new JPAProtobufFilterAndConverter(jpql, namedParams));
   }
}
