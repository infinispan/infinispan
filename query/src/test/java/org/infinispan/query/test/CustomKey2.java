package org.infinispan.query.test;

import java.io.Serializable;

import org.infinispan.query.Transformable;

@Transformable
public class CustomKey2 implements Serializable {
   int i, j, k;
   private static final long serialVersionUID = -8825579871900146417L;

   public CustomKey2(int i, int j, int k) {
      this.i = i;
      this.j = j;
      this.k = k;
   }

   public CustomKey2() {
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

      CustomKey2 that = (CustomKey2) o;

      if (i != that.i) return false;
      if (j != that.j) return false;
      if (k != that.k) return false;

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
