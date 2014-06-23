package org.infinispan.cli.interpreter.codec;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.util.logging.LogFactory;

/**
 * HotRodCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HotRodCodec extends AbstractCodec {
   public static final Log log = LogFactory.getLog(HotRodCodec.class, Log.class);
   Marshaller marshaller = new GenericJBossMarshaller(); // FIXME: assumes that clients will marshall using this

   @Override
   public String getName() {
      return "hotrod";
   }

   @Override
   public Object encodeKey(Object key) throws CodecException {
      if (key != null) {
         try {
            return marshaller.objectToByteBuffer(key);
         } catch (Exception e) {
            throw log.keyEncodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         try {
            return marshaller.objectToByteBuffer(value);
         } catch (Exception e) {
            throw log.valueEncodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeKey(Object key) throws CodecException {
      if (key != null) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) key);
         } catch (Exception e) {
            throw log.keyDecodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeValue(Object value) throws CodecException {
      if (value != null) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) value);
         } catch (Exception e) {
            throw log.valueDecodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }
}
