package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class SerializationMarshaller implements HotrodMarshaller {

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
         return ois.readObject();
      } catch (Exception e) {
         throw new HotRodClientException("Unexpected!", e);
      }
   }
}
