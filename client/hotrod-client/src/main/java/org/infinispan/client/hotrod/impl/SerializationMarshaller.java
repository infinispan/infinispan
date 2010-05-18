package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

/**
 * Default marshaller implementation based on object serialization.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class SerializationMarshaller implements HotRodMarshaller {

   private static Log log = LogFactory.getLog(SerializationMarshaller.class);

   @Override
   public byte[] marshallObject(Object toMarshall) {
      ByteArrayOutputStream result = new ByteArrayOutputStream(1000);
      try {
         ObjectOutputStream oos = new ObjectOutputStream(result);
         oos.writeObject(toMarshall);
         return result.toByteArray();
      } catch (IOException e) {
         throw new HotRodClientException("Unexpected!", e);
      }
   }

   @Override
   public Object readObject(byte[] bytes) {
      try {
         ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
         Object o = ois.readObject();
         if (log.isTraceEnabled()) {
            log.trace("Unmarshalled bytes: " + Arrays.toString(bytes) + " and returning object: " + o);
         }
         return o;
      } catch (Exception e) {
         throw new HotRodClientException("Unexpected!", e);
      }
   }
}
