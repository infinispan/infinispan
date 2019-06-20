package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.proto.ProtoStreamMarshaller;

/**
 * @author gustavonalle
 */
abstract class AbstractIckleFilterConverterFactory<T> {

   // This marshaller is able to handle primitive/scalar types only
   private static final ProtoStreamMarshaller paramMarshaller = new ProtoStreamMarshaller();

   private String unmarshallQueryString(Object[] params) {
      try {
         return (String) paramMarshaller.objectFromByteBuffer((byte[]) params[0]);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   private Map<String, Object> unmarshallParams(Object[] params) {
      Map<String, Object> namedParams = null;
      try {
         if (params.length > 1) {
            namedParams = new HashMap<>((params.length - 1) / 2);
            int i = 1;
            while (i < params.length) {
               String name = (String) paramMarshaller.objectFromByteBuffer((byte[]) params[i++]);
               Object value = paramMarshaller.objectFromByteBuffer((byte[]) params[i++]);
               namedParams.put(name, value);
            }
         }
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
      return namedParams;
   }

   public final T getFilterConverter(Object[] params) {
      String queryString = unmarshallQueryString(params);
      Map<String, Object> namedParams = unmarshallParams(params);
      return getFilterConverter(queryString, namedParams);
   }

   protected abstract T getFilterConverter(String queryString, Map<String, Object> namedParams);
}
