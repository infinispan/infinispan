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

import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheRemoveEntry;
import javax.cache.annotation.CacheResult;
import javax.cache.annotation.CacheValue;

/**
 * Address data access object
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class AddressDao {

   @CachePut(cacheName="address-cache")
   public String storeAddress(String personName, @CacheValue Address addr) {
      return String.format("%s lives in %s", personName, addr);
   }

   @CacheResult(cacheName="address-cache")
   public Address getAddress(String name) {
      return null; // No other source for addresses
   }

   @CacheRemoveEntry(cacheName="address-cache")
   public String removeAddress(String personName) {
      return String.format("%s no longer living there", personName);
   }

}
