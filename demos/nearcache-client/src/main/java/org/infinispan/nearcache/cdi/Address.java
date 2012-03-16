/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

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
