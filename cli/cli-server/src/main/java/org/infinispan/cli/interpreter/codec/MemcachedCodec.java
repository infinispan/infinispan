package org.infinispan.cli.interpreter.codec;

import java.nio.charset.Charset;

import org.infinispan.cli.interpreter.Interpreter;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.server.memcached.MemcachedValue;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * MemcachedCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class MemcachedCodec extends AbstractCodec {
   private static final Log log = LogFactory.getLog(Interpreter.class, Log.class);
   private Charset UTF8 = Charset.forName("UTF-8");

   @Override
   public String getName() {
      return "memcached";
   }

   @Override
   public Object encodeKey(Object key) {
      return key;
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         if (value instanceof MemcachedValue) {
            return value;
         } else if (value instanceof byte[]) {
            return new MemcachedValue((byte[])value, 1, 0);
         } else if (value instanceof String) {
            return new MemcachedValue(((String)value).getBytes(UTF8), 1, 0);
         } else {
            throw log.valueEncodingFailed(value.getClass().getName(), this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeKey(Object key) {
      return key;
   }

   @Override
   public Object decodeValue(Object value) {
      if (value != null) {
         MemcachedValue mv = (MemcachedValue)value;
         return new String(mv.data(), UTF8);
      } else {
         return null;
      }

   }

}
