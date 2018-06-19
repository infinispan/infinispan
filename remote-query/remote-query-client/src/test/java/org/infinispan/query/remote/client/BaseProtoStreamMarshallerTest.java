package org.infinispan.query.remote.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Date;

import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.junit.Test;

/**
 * A simple smoke test to ensure BaseProtoStreamMarshaller is able to handle primitive types besides actual protobuf
 * messages and enums.
 *
 * @author anistor@redhat.com
 */
public class BaseProtoStreamMarshallerTest {

   @Test
   public void testBasicTypesAreMarshallable() throws Exception {
      roundtrip("a");
      roundtrip('a');
      roundtrip(0);
      roundtrip(0L);
      roundtrip(0.0D);
      roundtrip(0.0F);
      roundtrip((byte) 0);
      roundtrip((short) 0);
      roundtrip(true);
      roundtrip(new Date(0));
      roundtrip(Instant.ofEpochMilli(System.currentTimeMillis()));
      roundtrip(new byte[0]);
   }

   private void roundtrip(Object in) throws Exception {
      BaseProtoStreamMarshaller marshaller = makeInstance();

      assertTrue(marshaller.isMarshallable(in));

      byte[] buffer = marshaller.objectToByteBuffer(in);
      assertNotNull(buffer);

      Object out = marshaller.objectFromByteBuffer(buffer);
      assertNotNull(out);

      assertEquals(in.getClass(), out.getClass());

      if (in instanceof byte[]) {
         assertArrayEquals((byte[]) in, (byte[]) out);
      } else {
         assertEquals(in, out);
      }
   }

   /**
    * BaseProtoStreamMarshaller is abstract. To test it we need to extend it and make it a concrete class.
    */
   private BaseProtoStreamMarshaller makeInstance() {
      return new BaseProtoStreamMarshaller() {

         private final SerializationContext serCtx = ProtobufUtil.newSerializationContext();

         @Override
         protected SerializationContext getSerializationContext() {
            return serCtx;
         }
      };
   }
}
