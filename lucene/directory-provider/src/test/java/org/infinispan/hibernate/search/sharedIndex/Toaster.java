package org.infinispan.hibernate.search.sharedIndex;

import javax.persistence.Entity;

import org.hibernate.search.annotations.Indexed;

@Entity
@Indexed(index = "device")
public class Toaster extends Device {

   public Toaster() {
      super("GE", "Scorcher5000", null);
   }

   public Toaster(String serialNumber) {
      super("GE", "Scorcher5000", serialNumber);
   }
}
