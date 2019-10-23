package org.infinispan.client.hotrod.marshall;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamConstants;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.CheckedInputStream;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.SerializationContext;

/**
 * @author Galder Zamarreño
 */
public final class MarshallerUtil {

   private static final Log log = LogFactory.getLog(MarshallerUtil.class, Log.class);

   private MarshallerUtil() {
   }

   /**
    * A convenience method to return the {@link SerializationContext} associated with the {@link ProtoStreamMarshaller}
    * configured on the provided {@link RemoteCacheManager}.
    *
    * @return the associated {@link SerializationContext}
    * @throws HotRodClientException if the cache manager is not started or is not configured to use a {@link ProtoStreamMarshaller}
    */
   public static SerializationContext getSerializationContext(RemoteCacheManager remoteCacheManager) {
      Marshaller marshaller = remoteCacheManager.getMarshaller();
      if (marshaller instanceof ProtoStreamMarshaller) {
         return ((ProtoStreamMarshaller) marshaller).getSerializationContext();
      }

      if (marshaller == null) {
         throw new HotRodClientException("The cache manager must be configured with a ProtoStreamMarshaller and must be started before attempting to retrieve the ProtoStream SerializationContext");
      }

      throw new HotRodClientException("The cache manager is not configured with a ProtoStreamMarshaller");
   }

   @SuppressWarnings("unchecked")
   public static <T> T bytes2obj(Marshaller marshaller, byte[] bytes, boolean objectStorage, ClassWhiteList whitelist) {
      if (bytes == null || bytes.length == 0) return null;
      try {
         Object ret = marshaller.objectFromByteBuffer(bytes);
         if (objectStorage) {
            // Server stores objects
            // No extra configuration is required for client in this scenario,
            // and no different marshaller should be required to deal with standard serialization.
            // So, if the unmarshalled object is still a byte[], it could be a standard
            // serialized object, so check for stream magic
            if (ret instanceof byte[] && isJavaSerialized((byte[]) ret)) {
               T ois = tryJavaDeserialize(bytes, (byte[]) ret, whitelist);
               if (ois != null)
                  return ois;
            }
         }

         return (T) ret;
      } catch (Exception e) {
         throw HOTROD.unableToUnmarshallBytes(Util.toHexString(bytes), e);
      }
   }

   public static <T> T tryJavaDeserialize(byte[] bytes, byte[] ret, ClassWhiteList whitelist) {
      try (ObjectInputStream ois = new CheckedInputStream(new ByteArrayInputStream(ret), whitelist)) {
         return (T) ois.readObject();
      } catch (CacheException ce) {
         throw ce;
      } catch (Exception ee) {
         if (log.isDebugEnabled())
            log.debugf("Standard deserialization not in use for %s", Util.printArray(bytes));
      }
      return null;
   }

   private static boolean isJavaSerialized(byte[] bytes) {
      if (bytes.length > 2) {
         short magic = (short) ((bytes[1] & 0xFF) + (bytes[0] << 8));
         return magic == ObjectStreamConstants.STREAM_MAGIC;
      }

      return false;
   }

   public static byte[] obj2bytes(Marshaller marshaller, Object o, boolean isKey, int estimateKeySize, int estimateValueSize) {
      try {
         return marshaller.objectToByteBuffer(o, isKey ? estimateKeySize : estimateValueSize);
      } catch (IOException ioe) {
         throw new HotRodClientException(
               "Unable to marshall object of type [" + o.getClass().getName() + "]", ioe);
      } catch (InterruptedException ie) {
         Thread.currentThread().interrupt();
         return null;
      }
   }

}
