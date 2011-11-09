/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TestAddress implements Address {
   int addressNum;

   String name;

   public void setName(String name) {
      this.name = name;
   }

   public TestAddress(int addressNum) {
      this.addressNum = addressNum;
   }

   public TestAddress(int addressNum, String name) {
      this.addressNum = addressNum;
      this.name = name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestAddress that = (TestAddress) o;

      if (addressNum != that.addressNum) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return addressNum;
   }

   @Override
   public String toString() {
      return "TestAddress#"+addressNum + (name != null? ("-" + name) : "");
   }

   public int compareTo(Object o) {
      return this.addressNum - ((TestAddress) o).addressNum;
   }
}
