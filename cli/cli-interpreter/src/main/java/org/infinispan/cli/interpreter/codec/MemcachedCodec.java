package org.infinispan.cli.interpreter.codec;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * MemcachedCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MetaInfServices(org.infinispan.cli.interpreter.codec.Codec.class)
public class MemcachedCodec extends AbstractCodec {
   private static final Log log = LogFactory.getLog(MemcachedCodec.class, Log.class);

   public MemcachedCodec() {
      try {
         Class.forName("org.infinispan.server.memcached.MemcachedServer");
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public String getName() {
      return "memcached";
   }

   @Override
   public void setWhiteList(ClassWhiteList whiteList) {
   }

   @Override
   public Object encodeKey(Object key) {
      if (key == null) return null;
      return key.toString().getBytes(UTF_8);
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         if (value instanceof String) {
            return ((String) value).getBytes(UTF_8);
         } else if (value instanceof byte[]) {
            return value;
         } else {
            throw log.valueEncodingFailed(value.getClass().getName(), this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeKey(Object key) {
      if (key == null) return null;
      if (key instanceof byte[]) {
         return new String((byte[]) key, UTF_8);
      }
      return key.toString();
   }

   @Override
   public Object decodeValue(Object value) {
      if (value != null) {
         return new String((byte[]) value, UTF_8);
      } else {
         return null;
      }
   }
}
