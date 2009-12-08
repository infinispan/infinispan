package org.infinispan.query;

import org.infinispan.CacheException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Warning, slow as a dog, uses serialization to get a byte representation of a class.  Implement your own!
 *
 * Repeat. It is HIGHLY RECOMMENDED THAT YOU PROVIDE YOUR OWN IMPLEMENTATION OF {@link org.infinispan.query.Transformer}
 *
 * @author Navin Surtani
 */
public class DefaultTransformer implements Transformer {
   @Override
   public Object fromString(String s) {
      //"sz:[b1, b2, b3, b4, ... ]"
      String sz = s.substring(0, s.indexOf(":"));
      byte[] buf = new byte[Integer.parseInt(sz)];
      String sub = s.substring(s.indexOf(":") + 1);
      String tokens = sub.replace("[", "").replace("]", "");
      StringTokenizer st = new StringTokenizer(tokens, ",");
      int i = 0;
      while (st.hasMoreTokens()) {
         String token = st.nextToken().trim();
         byte b = Byte.parseByte(token);
         buf[i++] = b;
      }

      ObjectInputStream ois = null;
      try {
         ois = new ObjectInputStream(new ByteArrayInputStream(buf));
         Object o = ois.readObject();
         ois.close();
         return o;
      } catch (Exception e) {
         throw new CacheException (e);
      }

   }

   @Override
   public String toString(Object customType) {
      try {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos);
         oos.writeObject(customType);
         oos.close();
         baos.close();
         byte[] b = baos.toByteArray();
         return b.length + ":" + Arrays.toString(b);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }
}
