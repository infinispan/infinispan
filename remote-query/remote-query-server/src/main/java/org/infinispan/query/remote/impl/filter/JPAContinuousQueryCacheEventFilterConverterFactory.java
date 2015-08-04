package org.infinispan.query.remote.impl.filter;

import org.infinispan.commons.CacheException;
import org.infinispan.filter.NamedFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;
import org.kohsuke.MetaInfServices;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@NamedFactory(name = JPAContinuousQueryCacheEventFilterConverterFactory.FACTORY_NAME)
@MetaInfServices
public final class JPAContinuousQueryCacheEventFilterConverterFactory implements CacheEventFilterConverterFactory {

   public static final String FACTORY_NAME = "continuous-query-filter-converter-factory";

   private final BaseProtoStreamMarshaller paramMarshaller = new BaseProtoStreamMarshaller() {

      private final SerializationContext serializationContext = ProtobufUtil.newSerializationContext(new Configuration.Builder().build());

      @Override
      protected SerializationContext getSerializationContext() {
         return serializationContext;
      }
   };

   @Override
   public CacheEventFilterConverter<?, ?, ?> getFilterConverter(Object[] params) {
      String jpql;
      Map<String, Object> namedParams = null;
      try {
         jpql = (String) paramMarshaller.objectFromByteBuffer((byte[]) params[0]);
         if (params.length > 1) {
            namedParams = new HashMap<String, Object>((params.length - 1) / 2);
            int i = 1;
            while (i < params.length) {
               String name = (String) paramMarshaller.objectFromByteBuffer((byte[]) params[i++]);
               Object value = paramMarshaller.objectFromByteBuffer((byte[]) params[i++]);
               namedParams.put(name, value);
            }
         }
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheException(e);
      }
      return new JPAContinuousQueryProtobufCacheEventFilterConverter(jpql, namedParams, ProtobufMatcher.class);
   }
}

