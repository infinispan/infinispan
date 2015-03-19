package org.infinispan.hibernate.search.sharedIndex;

import org.hibernate.search.annotations.Indexed;

import javax.persistence.Entity;

@Entity
@Indexed(index = "device")
public class Robot extends Device {

   public Robot() {
      super("Galactic", "Infinity1000", null);
   }

   public Robot(String serialNumber) {
      super("Galactic", "Infinity1000", serialNumber);
   }
}
