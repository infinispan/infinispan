package org.infinispan.test;

public class FactoryError {
   private final String message;

   public FactoryError(String message) {
      this.message = message;
   }

   public void fail() {
      throw new IllegalStateException(message);
   }
}
