package org.infinispan.hibernate.search.sharedIndex;

import org.hibernate.search.annotations.Indexed;

import javax.persistence.Entity;

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
