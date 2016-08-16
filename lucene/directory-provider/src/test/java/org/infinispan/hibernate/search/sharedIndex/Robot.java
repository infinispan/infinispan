package org.infinispan.hibernate.search.sharedIndex;

import javax.persistence.Entity;

import org.hibernate.search.annotations.Indexed;

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
