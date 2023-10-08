package org.infinispan.query.distributed;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.Transformable;
import org.infinispan.query.Transformer;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@Transformable(transformer = NonSerializableKeyType.CustomTransformer.class)
public class NonSerializableKeyType {

   @ProtoField(number = 1)
   final String keyValue;

   @ProtoFactory
   NonSerializableKeyType(final String keyValue) {
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
