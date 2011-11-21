package org.infinispan.configuration.cache;

public class UnsafeConfiguration {

   private final boolean unreliableReturnValues;

   UnsafeConfiguration(boolean unreliableReturnValues) {
      this.unreliableReturnValues = unreliableReturnValues;
   }
   
   public boolean unreliableReturnValues() {
      return unreliableReturnValues;
   }
   
}
