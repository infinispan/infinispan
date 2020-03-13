package org.infinispan.commons.util;

/**
 * This class can be used to pass an argument by reference.
 * @param <T> The wrapped type.
 *
 * @author Dan Berindei
 * @since 5.1
 */
public class ByRef<T> {
   private T ref;

   public ByRef(T t) {
      ref = t;
   }

   public static <T> ByRef<T> create(T t) {
      return new ByRef<T>(t);
   }

   public T get() {
      return ref;
   }

   public void set(T t) {
      ref = t;
   }

   /**
    * Implementation for primitive type
    */
   public static class Boolean {
      boolean ref;

      public Boolean(boolean b) {
         ref = b;
      }

      public boolean get() {
         return ref;
      }

      public void set(boolean b) {
         this.ref = b;
      }
   }

   public static class Integer {
      int ref;

      public Integer(int i) {
         ref = i;
      }

      public int get() {
         return ref;
      }

      public void set(int i) {
         ref = i;
      }

      public void inc() {
         ref++;
      }
   }

}
