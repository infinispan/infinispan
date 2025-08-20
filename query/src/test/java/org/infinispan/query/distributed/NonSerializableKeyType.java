package org.infinispan.query.distributed;

import java.util.Objects;

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
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      NonSerializableKeyType that = (NonSerializableKeyType) o;
      return Objects.equals(keyValue, that.keyValue);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(keyValue);
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
