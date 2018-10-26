package org.infinispan.query.distributed;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.query.Transformable;
import org.infinispan.query.Transformer;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@SerializeWith(NonSerializableKeyType.CustomExternalizer.class)
@Transformable(transformer = NonSerializableKeyType.CustomTransformer.class)
public class NonSerializableKeyType {

   public final String keyValue;

   public NonSerializableKeyType(final String keyValue) {
      this.keyValue = keyValue;
   }

   @Override
   public int hashCode() {
      return keyValue.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      NonSerializableKeyType other = (NonSerializableKeyType) obj;
      if (keyValue == null) {
         if (other.keyValue != null)
            return false;
      } else if (!keyValue.equals(other.keyValue))
         return false;
      return true;
   }

   public static class CustomExternalizer implements Externalizer<NonSerializableKeyType> {
      @Override
      public void writeObject(ObjectOutput output, NonSerializableKeyType object) throws IOException {
         output.writeUTF(object.keyValue);
      }

      @Override
      public NonSerializableKeyType readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new NonSerializableKeyType(input.readUTF());
      }
   }

   public static class CustomTransformer implements Transformer {
      @Override
      public Object fromString(String s) {
         return new NonSerializableKeyType(s);
      }

      @Override
      public String toString(Object customType) {
         return ((NonSerializableKeyType)customType).keyValue;
      }
   }

}
