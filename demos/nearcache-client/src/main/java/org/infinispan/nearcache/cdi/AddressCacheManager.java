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

import org.infinispan.Cache;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheRemoveAll;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Global operations for the address cache
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Named @ApplicationScoped
public class AddressCacheManager {

   @Inject @AddressCache
   private Cache<CacheKey, Address> cache;

   public String[] getCachedValues() {
      List<String> values = new ArrayList<String>();
      for (Map.Entry<CacheKey, Address> entry : cache.entrySet())
         values.add(String.format("%s -> %s", entry.getKey(), entry.getValue()));

      return values.toArray(new String[values.size()]);
   }

   public int getNumberOfEntries() {
      return cache.size();
   }

   @CacheRemoveAll(cacheName = "address-cache")
   public void clearCache() {}

}
