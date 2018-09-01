package org.infinispan.query.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.Transformer;

/**
 * WARNING, slow as a senile dog, uses Java Serialization and base64 encoding to get a String representation of an
 * Object. It is highly recommended that you provide your own implementation of {@link
 * org.infinispan.query.Transformer}.
 *
 * @author Navin Surtani
 * @author anistor@redhat.com
 */
public final class DefaultTransformer implements Transformer {

   private static final Log log = LogFactory.getLog(DefaultTransformer.class);

   @Override
   public Object fromString(String str) {
      byte[] objBytes = Base64.getDecoder().decode(str);
      try {
         ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(objBytes));
         return ois.readObject();
      } catch (IOException | ClassNotFoundException e) {
         log.error("Error while decoding object", e);
         throw new CacheException(e);
      }
   }

   @Override
   public String toString(Object obj) {
      if (obj instanceof Serializable) {
         try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(Base64.getEncoder().wrap(baos));
            oos.writeObject(obj);
            oos.close();
            byte[] base64encoded = baos.toByteArray();
            return new String(base64encoded, StandardCharsets.UTF_8);
         } catch (IOException e) {
            log.error("Error while encoding object", e);
            throw new CacheException(e);
         }
      } else {
         throw new IllegalArgumentException("Expected " + obj.getClass() + " to be Serializable!");
      }
   }
}
