package org.infinispan.remoting.transport;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;

/**
 * Represents the local node's address.
 *
 * @since 9.0
 */
public class LocalModeAddress implements Address {

   public static final LocalModeAddress INSTANCE = new LocalModeAddress();

   private LocalModeAddress() {
   }

   @Override
   public String toString() {
      return "<local>";
   }

   @Override
   public int compareTo(Address o) {
      return o == this ? 0 : -1;
   }

   /**
    * LocalModeAddress can be serialized when persisting CommandInvocationId
    */
   public static class Externalizer implements AdvancedExternalizer<LocalModeAddress> {

      @Override
      public Set<Class<? extends LocalModeAddress>> getTypeClasses() {
         return Util.asSet(LocalModeAddress.class);
      }

      @Override
      public Integer getId() {
         return Ids.LOCAL_MODE_ADDRESS;
      }

      @Override
      public void writeObject(ObjectOutput output, LocalModeAddress object) throws IOException {
      }

      @Override
      public LocalModeAddress readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }
}
