package org.infinispan.hibernate.search.sharedIndex;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public abstract class Device {

   public Device(String manufacturer, String model, String serialNumber) {
      this.manufacturer = manufacturer;
      this.model = model;
      this.serialNumber = serialNumber;
   }

   @Id
   @GeneratedValue
   public Long id;

   @Field(analyze = Analyze.NO)
   @Column(name = "mfg")
   public String manufacturer = "";

   @Field(analyze = Analyze.NO)
   @Column(name = "model")
   public String model = "";

   @Field(analyze = Analyze.NO)
   @Column(name = "serial_number")
   public String serialNumber;

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((manufacturer == null) ? 0 : manufacturer.hashCode());
      result = prime * result + ((model == null) ? 0 : model.hashCode());
      result = prime * result + ((serialNumber == null) ? 0 : serialNumber.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof Device)) {
         return false;
      }
      Device other = (Device) obj;

      if (manufacturer == null) {
         if (other.manufacturer != null) {
            return false;
         }
      } else {
         if (!manufacturer.equals(other.manufacturer)) {
            return false;
         }
      }
      if (model == null) {
         if (other.model != null) {
            return false;
         }
      } else {
         if (!model.equals(other.model)) {
            return false;
         }
      }
      if (serialNumber == null) {
         if (other.serialNumber != null) {
            return false;
         }
      } else {
         if (!serialNumber.equals(other.serialNumber)) {
            return false;
         }
      }
      return true;
   }
}
