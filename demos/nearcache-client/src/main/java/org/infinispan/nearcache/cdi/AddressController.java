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

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * CDI controller
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Named @RequestScoped
public class AddressController {

   @Inject
   private AddressDao dao;
   private String name;
   private String street;
   private String result;

   public void store() {
      result = dao.storeAddress(name, new Address().street(street));
   }

   public void get() {
      dao.getAddress(name);
   }

   public void remove() {
      result = dao.removeAddress(name);
   }

   public String getName() { return name; }
   public void setName(String name) { this.name = name; }
   public String getStreet() { return street; }
   public void setStreet(String street) { this.street = street; }
   public String getResult() { return result; }

}
