package org.infinispan.query.test;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.Transformable;

@Transformable
public class CustomKey2 implements Serializable {

   private static final long serialVersionUID = -1;

   private final int i, j, k;

   @ProtoFactory
   public CustomKey2(int i, int j, int k) {
      this.i = i;
      this.j = j;
      this.k = k;
   }

   @ProtoField(number = 1, defaultValue = "0")
   public int getI() {
      return i;
   }

   @ProtoField(number = 2, defaultValue = "0")
   public int getJ() {
      return j;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public int getK() {
      return k;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CustomKey2 other = (CustomKey2) o;
      return i == other.i && j == other.j && k == other.k;
   }

   @Override
   public int hashCode() {
      return 31 * (31 * i + j) + k;
   }
}
