package org.infinispan.cli.interpreter.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * RestCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MetaInfServices(org.infinispan.cli.interpreter.codec.Codec.class)
public class RestCodec extends AbstractCodec {
   public static final Log log = LogFactory.getLog(RestCodec.class, Log.class);

   public RestCodec() {
      try {
         Class.forName("org.infinispan.rest.RestServer");
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public String getName() {
      return "rest";
   }

   @Override
   public Object encodeKey(Object key) {
      return key.toString().getBytes();
   }

   @Override
   public Object encodeValue(Object value) {
      if (value == null) return null;
      if (value instanceof String) {
         return ((String) value).getBytes(UTF_8);
      }
      if (value instanceof byte[]) {
         return value;
      }
      return value.toString();
   }

   @Override
   public Object decodeKey(Object key) {
      return key;
   }

   @Override
   public Object decodeValue(Object value) {
      if (value == null) return null;
      return value instanceof byte[] ? new String((byte[]) value, UTF_8) : value.toString();
   }

}
