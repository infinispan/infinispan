package org.infinispan.query.test;

import org.infinispan.query.Transformable;

@Transformable(transformer = CustomTransformer.class)
public class CustomKey {

   int i, j, k;

   public CustomKey(int i, int j, int k) {
      this.i = i;
      this.j = j;
      this.k = k;
   }

   public CustomKey() {
   }

   public int getI() {
      return i;
   }

   public void setI(int i) {
      this.i = i;
   }

   public int getJ() {
      return j;
   }

   public void setJ(int j) {
      this.j = j;
   }

   public int getK() {
      return k;
   }

   public void setK(int k) {
      this.k = k;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomKey customKey = (CustomKey) o;

      if (i != customKey.i) return false;
      if (j != customKey.j) return false;
      if (k != customKey.k) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = i;
      result = 31 * result + j;
      result = 31 * result + k;
      return result;
   }
}
