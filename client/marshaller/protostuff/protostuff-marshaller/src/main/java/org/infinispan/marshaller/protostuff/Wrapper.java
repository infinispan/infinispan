package org.infinispan.marshaller.protostuff;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class Wrapper {
   Object object;

   Wrapper(Object object) {
      this.object = object;
   }
}
