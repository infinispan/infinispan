package org.infinispan.api.client.impl;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public final class Address {

   @ProtoDoc("@Field")
   @ProtoField(number = 1, required = true)
   String number;

   @ProtoDoc("@Field")
   @ProtoField(number = 2, required = true)
   String street;

   @ProtoDoc("@Field")
   @ProtoField(number = 3, required = true)
   String postalCode;

   @ProtoDoc("@Field")
   @ProtoField(number = 4, required = true)
   String town;

   @ProtoDoc("@Field")
   @ProtoField(number = 5, required = true)
   String country;

   public Address() {

   }

   public Address(String number, String street, String postalCode, String town, String country) {
      this.number = number;
      this.street = street;
      this.postalCode = postalCode;
      this.town = town;
      this.country = country;
   }

   @Override
   public String toString() {
      return "Address{" +
            "number='" + number + '\'' +
            ", street='" + street + '\'' +
            ", postalCode='" + postalCode + '\'' +
            ", town='" + town + '\'' +
            ", country='" + country + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Address address = (Address) o;
      return Objects.equals(number, address.number) &&
            Objects.equals(street, address.street) &&
            Objects.equals(postalCode, address.postalCode) &&
            Objects.equals(town, address.town) &&
            Objects.equals(country, address.country);
   }

   @Override
   public int hashCode() {
      return Objects.hash(number, street, postalCode, town, country);
   }
}
