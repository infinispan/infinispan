package org.infinispan.rest.operations.mediatypes.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class SerializationUtil {

   private SerializationUtil() {

   }

   public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
         try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
         }
      }
   }

   public static byte[] toByteArray(Object object) throws IOException {
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
         try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
         }
         return bos.toByteArray();
      }
   }

}
