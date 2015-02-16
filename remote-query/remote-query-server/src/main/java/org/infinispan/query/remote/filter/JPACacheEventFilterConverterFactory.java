package org.infinispan.query.remote.filter;

import org.infinispan.commons.CacheException;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

import java.io.IOException;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@NamedFactory(name = JPACacheEventFilterConverterFactory.FACTORY_NAME)
public class JPACacheEventFilterConverterFactory implements CacheEventFilterFactory, CacheEventConverterFactory {

   public static final String FACTORY_NAME = "query-dsl-filter-converter-factory";

   private final BaseProtoStreamMarshaller paramMarshaller = new BaseProtoStreamMarshaller() {

      private final SerializationContext serializationContext = ProtobufUtil.newSerializationContext(new Configuration.Builder().build());

      @Override
      protected SerializationContext getSerializationContext() {
         return serializationContext;
      }
   };

   @Override
   public CacheEventFilter<byte[], byte[]> getFilter(Object[] params) {
      return getCacheEventFilterConverter(params);
   }

   @Override
   public CacheEventConverter<byte[], byte[], byte[]> getConverter(Object[] params) {
      return getCacheEventFilterConverter(params);
   }

   private JPAProtobufCacheEventFilterConverter getCacheEventFilterConverter(Object[] params) {
      String jpql;
      try {
         jpql = (String) paramMarshaller.objectFromByteBuffer((byte[]) params[0]);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheException(e);
      }
      return new JPAProtobufCacheEventFilterConverter(new JPAFilterAndConverter<byte[], byte[]>(jpql, ProtobufMatcher.class));
   }
}

