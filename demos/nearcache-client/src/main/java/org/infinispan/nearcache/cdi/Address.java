package org.infinispan.nearcache.cdi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An address
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Address implements Externalizable {
   String street;

   Address street(String street) {
      this.street = street;
      return this;
   }

   @Override
   public String toString() {
      return street;
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(street);
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      street = (String) in.readObject();
   }

}
