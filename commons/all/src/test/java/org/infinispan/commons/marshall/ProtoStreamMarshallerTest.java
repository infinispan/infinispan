package org.infinispan.commons.marshall;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Date;

import org.junit.Test;

/**
 * A simple smoke test to ensure BaseProtoStreamMarshaller is able to handle primitive types besides actual protobuf
 * messages and enums.
 *
 * @author anistor@redhat.com
 */
public class ProtoStreamMarshallerTest {

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
      roundtrip(Instant.now());
      roundtrip(new byte[0]);
   }

   private void roundtrip(Object in) throws Exception {
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();

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
}
