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
package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * An abstract consistent hash implementation that handles common implementations of certain methods.  In particular,
 * default implementations of {@link #locateAll(java.util.Collection, int)} and {@link #isKeyLocalToAddress(org.infinispan.remoting.transport.Address, Object, int)}.
 * <p />
 * The versions provided here are relatively inefficient in that they call {@link #locate(Object, int)} first (and
 * sometimes in a loop).  Depending on the algorithm used, there may be more efficient ways to achieve the same results
 * and in such cases the methods provided here should be overridden.
 * <p />
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractConsistentHash implements ConsistentHash {

   protected volatile Set<Address> caches;

   @Override
   public void setCaches(Set<Address> caches) {
      this.caches = new TreeSet<Address>(new Comparator<Address>() {
         @Override
         public int compare(Address o1, Address o2) {
            return o1.hashCode() - o2.hashCode();
         }
      });

      for (Address a: caches) this.caches.add(a);
   }

   @Override
   public Set<Address> getCaches() {
      return caches;
   }

   @Override
   public Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount) {
      Map<Object, List<Address>> locations = new HashMap<Object, List<Address>>();
      for (Object k : keys) locations.put(k, locate(k, replCount));
      return locations;
   }

   @Override
   public boolean isKeyLocalToAddress(Address a, Object key, int replCount) {
      // simple, brute-force impl
      return locate(key, replCount).contains(a);
   }

   @Override
   public void setTopologyInfo(TopologyInfo topologyInfo) {
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " {" +
            "caches=" + caches +
            '}';
   }
}
