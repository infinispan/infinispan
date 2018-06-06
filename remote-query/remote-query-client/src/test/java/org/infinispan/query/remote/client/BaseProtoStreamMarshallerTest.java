package org.infinispan.query.remote.client;

import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Date;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.junit.Test;

/**
 * A simple smoke test to ensure BaseProtoStreamMarshaller is able to handle primitive types besides actual protobuf
 * messages and enums.
 *
 * @author anistor@redhat.com
 */
public class BaseProtoStreamMarshallerTest {

   @Test
   public void testBasicTypesAreMarshallable() {
      BaseProtoStreamMarshaller marshaller = makeIstance();

      assertTrue(marshaller.isMarshallable("a"));
      assertTrue(marshaller.isMarshallable('a'));
      assertTrue(marshaller.isMarshallable(0));
      assertTrue(marshaller.isMarshallable(0L));
      assertTrue(marshaller.isMarshallable(0.0D));
      assertTrue(marshaller.isMarshallable(0.0F));
      assertTrue(marshaller.isMarshallable((byte) 0));
      assertTrue(marshaller.isMarshallable((short) 0));
      assertTrue(marshaller.isMarshallable(true));
      assertTrue(marshaller.isMarshallable(new Date(0)));
      assertTrue(marshaller.isMarshallable(Instant.now()));
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
