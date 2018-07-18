package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.Serializable;



/**
 * A test pojo that is marshalled using Infinispan's
 * {@link org.infinispan.commons.marshall.Externalizer} which is annotated with
 * {@link SerializeWith}
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@SerializeWith(PojoWithSerializeWith.Externalizer.class)
public class PojoWithSerializeWith {

   final PojoWithAttributes pojo;

   public PojoWithSerializeWith(int age, String key) {
      this.pojo = new PojoWithAttributes(age, key);
   }

   public PojoWithSerializeWith(PojoWithAttributes pojo) {
      this.pojo = pojo;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PojoWithSerializeWith that = (PojoWithSerializeWith) o;

      return !(pojo != null ? !pojo.equals(that.pojo) : that.pojo != null);
   }

   @Override
   public int hashCode() {
      return pojo != null ? pojo.hashCode() : 0;
   }

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<PojoWithSerializeWith>, Serializable {
      @Override
      public void writeObject(UserObjectOutput output, PojoWithSerializeWith object) throws IOException {
         PojoWithAttributes.writeObject(output, object.pojo);
      }

      @Override
      public PojoWithSerializeWith readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new PojoWithSerializeWith(PojoWithAttributes.readObject(input));
      }
   }
}
