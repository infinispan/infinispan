package org.infinispan.query.remote.client;

import static org.junit.Assert.assertTrue;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.junit.Test;

/**
 * Ensure BaseProtoStreamMarshaller is able to handle primitive types besides actual protobuf messages and enums.
 *
 * @author anistor@redhat.com
 */
public class BaseProtoStreamMarshallerTest {

   @Test
   public void testBasicTypesAreMarshallable() {
      BaseProtoStreamMarshaller marshaller = makeIstance();

      assertTrue(marshaller.isMarshallable(""));
      assertTrue(marshaller.isMarshallable(0));
      assertTrue(marshaller.isMarshallable(0L));
      assertTrue(marshaller.isMarshallable(0.0));
      assertTrue(marshaller.isMarshallable(0.0f));
      assertTrue(marshaller.isMarshallable(true));
      assertTrue(marshaller.isMarshallable(new byte[0]));
   }

   /**
    * BaseProtoStreamMarshaller is abstract. To test it we need to extend it and make it a concrete class.
    */
   private BaseProtoStreamMarshaller makeIstance() {
      return new BaseProtoStreamMarshaller() {

         private final SerializationContext serCtx = ProtobufUtil.newSerializationContext(Configuration.builder().build());

         @Override
         protected SerializationContext getSerializationContext() {
            return serCtx;
         }
      };
   }
}
