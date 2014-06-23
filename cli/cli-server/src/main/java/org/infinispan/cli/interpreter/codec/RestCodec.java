package org.infinispan.cli.interpreter.codec;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.util.logging.LogFactory;

/**
 * RestCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RestCodec extends AbstractCodec {
   public static final Log log = LogFactory.getLog(RestCodec.class, Log.class);

   @Override
   public String getName() {
      return "rest";
   }

   @Override
   public Object encodeKey(Object key) throws CodecException {
      return key;
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         if (value instanceof MIMECacheEntry) {
            return value;
         } else if (value instanceof String) {
            return new MIMECacheEntry("text/plain", ((String)value).getBytes());
         } else if (value instanceof byte[]) {
            return new MIMECacheEntry("application/binary", (byte[])value);
         } else {
            throw log.valueEncodingFailed(value.getClass().getName(), this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeKey(Object key) throws CodecException {
      return key;
   }

   @Override
   public Object decodeValue(Object value) throws CodecException {
      if (value != null) {
         MIMECacheEntry e = (MIMECacheEntry)value;
         if (e.contentType.startsWith("text/") || e.contentType.equals("application/xml") || e.contentType.equals("application/json")) {
            return new String(e.data);
         } else {
            return e.data;
         }
      } else {
         return null;
      }
   }

}
