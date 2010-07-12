package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.HotRodMarshaller;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Default marshaller implementation based on object serialization.
 * Supports two configuration elements:
 * <ul>
 *  <li>marshaller.default-array-size.key - the size of the {@link ExposedByteArrayOutputStream} that will be
 *   created for marshalling keys</li>
 *  <li> marshaller.default-array-size.value - the size of the {@link ExposedByteArrayOutputStream} that will be
 *   created for marshalling values
 *  </li>
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class SerializationMarshaller implements HotRodMarshaller {

   private static Log log = LogFactory.getLog(SerializationMarshaller.class);

   private volatile int defaultArraySizeForKey = 128;
   private volatile int defaultArraySizeForValue = 256;

   @Override
   public void init(Properties config) {
      if (config.contains("marshaller.default-array-size.key")) {
         defaultArraySizeForKey = Integer.parseInt(config.getProperty("marshaller.default-array-size.key"));
      }
      if (config.contains("marshaller.default-array-size.value")) {
         defaultArraySizeForValue = Integer.parseInt(config.getProperty("marshaller.default-array-size.value"));
      }
   }

   @Override
   public byte[] marshallObject(Object toMarshall, boolean isKeyHint) {
      ExposedByteArrayOutputStream result = getByteArray(isKeyHint);
      try {
         ObjectOutputStream oos = new ObjectOutputStream(result);
         oos.writeObject(toMarshall);
         return result.toByteArray();
      } catch (IOException e) {
         throw new HotRodClientException("Unexpected!", e);
      }
   }

   private ExposedByteArrayOutputStream getByteArray(boolean keyHint) {
      if (keyHint) {
         return new ExposedByteArrayOutputStream(defaultArraySizeForKey);
      } else {
         return new ExposedByteArrayOutputStream(defaultArraySizeForValue);
      }
   }

   @Override
   public Object readObject(byte[] bytes) {
      try {
         ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
         Object o = ois.readObject();
         if (log.isTraceEnabled()) {
            log.trace("Unmarshalled bytes: " + Util.printArray(bytes, false) + " and returning object: " + o);
         }
         return o;
      } catch (Exception e) {
         throw new HotRodClientException("Unexpected!", e);
      }
   }
}
