package org.infinispan.cli.interpreter.codec;

import java.nio.charset.Charset;

import org.infinispan.cli.interpreter.Interpreter;
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
   private static final Log log = LogFactory.getLog(Interpreter.class, Log.class);
   private Charset UTF8 = Charset.forName("UTF-8");

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
      return key;
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         if (value instanceof String) {
            return ((String) value).getBytes(UTF8);
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
      return key;
   }

   @Override
   public Object decodeValue(Object value) {
      if (value != null) {
         return new String((byte[]) value, UTF8);
      } else {
         return null;
      }

   }

}
