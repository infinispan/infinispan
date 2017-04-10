package org.infinispan.rest.server.operations.mediatypes.printers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

class DeserializationUtil {

   private DeserializationUtil() {

   }

   public static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
         try (ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
         }
      }
   }

}
