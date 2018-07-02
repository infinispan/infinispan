package org.infinispan.cli.interpreter.codec;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

/**
 * HotRodCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MetaInfServices(org.infinispan.cli.interpreter.codec.Codec.class)
public class HotRodCodec extends AbstractCodec {
   public static final Log log = LogFactory.getLog(HotRodCodec.class, Log.class);
   Marshaller marshaller; // FIXME: assumes that clients will marshall using this
   private ClassWhiteList classWhiteList;

   public HotRodCodec() {
      try {
         Class.forName("org.infinispan.server.hotrod.HotRodServer");
      } catch (ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }

   private Marshaller getMarshaller() {
      if (marshaller == null) {
         if (classWhiteList == null) throw log.whiteListNotFound();
         return new GenericJBossMarshaller(classWhiteList);
      }
      return marshaller;
   }

   @Override
   public String getName() {
      return "hotrod";
   }

   @Override
   public void setWhiteList(ClassWhiteList whiteList) {
      this.classWhiteList = whiteList;
   }

   @Override
   public Object encodeKey(Object key) throws CodecException {
      if (key != null) {
         try {
            return getMarshaller().objectToByteBuffer(key);
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
            return getMarshaller().objectToByteBuffer(value);
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
            return getMarshaller().objectFromByteBuffer((byte[]) key);
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
            return getMarshaller().objectFromByteBuffer((byte[]) value);
         } catch (Exception e) {
            throw log.valueDecodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }
}
