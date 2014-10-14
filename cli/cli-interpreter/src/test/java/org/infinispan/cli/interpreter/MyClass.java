package org.infinispan.cli.interpreter;

public class MyClass {
   int i;
   String s;
   boolean b;
   MyClass x;
   double d;

   public int getI() {
      return i;
   }

   public void setI(int i) {
      this.i = i;
   }

   public String getS() {
      return s;
   }

   public void setS(String s) {
      this.s = s;
   }

   public boolean isB() {
      return b;
   }

   public void setB(boolean b) {
      this.b = b;
   }

   public MyClass getX() {
      return x;
   }

   public void setX(MyClass x) {
      this.x = x;
   }

   @Override
   public String toString() {
      return "MyClass [i=" + i + ", s=" + s + ", b=" + b + ", x=" + x + ", d=" + d + "]";
   }
}
