package org.infinispan.jcache.embedded;

import org.infinispan.marshall.core.MarshalledValue;

/**
 * Helper class for dealing with MarshalledValues
 */
class MarshalledValues {

   static <T> T extract(Object obj) {
      return as((obj instanceof MarshalledValue ? ((MarshalledValue) obj).get() : obj));
   }

   @SuppressWarnings("unchecked")
   private static <T> T as(Object obj) {
      return (T) obj;
   }

   private MarshalledValues() {
      // Cannot be instantiated, it's just a holder class
   }
}
