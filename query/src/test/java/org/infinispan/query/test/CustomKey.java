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
      CustomKey other = (CustomKey) o;
      return i == other.i && j == other.j && k == other.k;
   }

   @Override
   public int hashCode() {
      return 31 * (31 * i + j) + k;
   }
}
