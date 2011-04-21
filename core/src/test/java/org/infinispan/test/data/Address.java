/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.test.data;

import java.io.Serializable;

public class Address implements Serializable {
   private static final long serialVersionUID = 5943073369866339615L;

   String street = null;
   String city = "San Jose";
   int zip = 0;

   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   public String getCity() {
      return city;
   }

   public void setCity(String city) {
      this.city = city;
   }

   public int getZip() {
      return zip;
   }

   public void setZip(int zip) {
      this.zip = zip;
   }

   public String toString() {
      return "street=" + getStreet() + ", city=" + getCity() + ", zip=" + getZip();
   }

//    public Object writeReplace() {
//	return this;
//    }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final Address address = (Address) o;

      if (zip != address.zip) return false;
      if (city != null ? !city.equals(address.city) : address.city != null) return false;
      if (street != null ? !street.equals(address.street) : address.street != null) return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (street != null ? street.hashCode() : 0);
      result = 29 * result + (city != null ? city.hashCode() : 0);
      result = 29 * result + zip;
      return result;
   }
}
