package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;

/**
 * @author gustavonalle
 */
class IckleFilterConverterUtils {

   // This marshaller is able to handle primitive/scalar types only
   private static final BaseProtoStreamMarshaller paramMarshaller = new BaseProtoStreamMarshaller() {

      private final SerializationContext serializationContext = ProtobufUtil.newSerializationContext();

      @Override
      protected SerializationContext getSerializationContext() {
         return serializationContext;
      }
   };

   private IckleFilterConverterUtils() {
   }

   static String unmarshallQueryString(Object[] params) {
      try {
         return (String) paramMarshaller.objectFromByteBuffer((byte[]) params[0]);
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   static Map<String, Object> unmarshallParams(Object[] params) {
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
}
