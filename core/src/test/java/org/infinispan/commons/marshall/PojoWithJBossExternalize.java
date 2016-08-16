package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.jboss.marshalling.Creator;
import org.jboss.marshalling.Externalize;

/**
 * A test pojo that is marshalled using JBoss Marshalling's
 * {@link org.jboss.marshalling.Externalizer} which is annotated with
 * {@link Externalize}
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Externalize(PojoWithJBossExternalize.Externalizer.class)
public class PojoWithJBossExternalize {
   final PojoWithAttributes pojo;

   public PojoWithJBossExternalize(int age, String key) {
      this.pojo = new PojoWithAttributes(age, key);
   }

   PojoWithJBossExternalize(PojoWithAttributes pojo) {
      this.pojo = pojo;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PojoWithJBossExternalize that = (PojoWithJBossExternalize) o;

      if (pojo != null ? !pojo.equals(that.pojo) : that.pojo != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return pojo != null ? pojo.hashCode() : 0;
   }

   public static class Externalizer implements org.jboss.marshalling.Externalizer {
      @Override
      public void writeExternal(Object subject, ObjectOutput output) throws IOException {
         PojoWithAttributes.writeObject(output, ((PojoWithJBossExternalize) subject).pojo);
      }

      @Override
      public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator) throws IOException, ClassNotFoundException {
         return new PojoWithJBossExternalize(PojoWithAttributes.readObject(input));
      }

      @Override
      public void readExternal(Object subject, ObjectInput input) throws IOException, ClassNotFoundException {
      }
   }
}
