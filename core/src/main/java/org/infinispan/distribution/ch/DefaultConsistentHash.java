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

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultConsistentHash extends AbstractWheelConsistentHash {

   public DefaultConsistentHash() {
   }

   public DefaultConsistentHash(Hash hash) {
      setHashFunction(hash);
   }

   public List<Address> locate(Object key, int replCount) {
      return locateInternal(key, replCount, null);
   }

   @Override
   public boolean isKeyLocalToAddress(Address target, Object key, int replCount) {
      return locateInternal(key, replCount, target) == null;
   }

   /**
    * Locate <code>replCount</code> owners for key <code>key</code> and return the list.
    * If one of the owners is identical to <code>target</code>, return <code>null</code> instead.
    */
   private List<Address> locateInternal(Object key, int replCount, Address target) {
      int actualReplCount = Math.min(replCount, caches.size());
      int normalizedHash;
      normalizedHash = getNormalizedHash(getGrouping(key));

      List<Address> owners = new ArrayList<Address>(replCount);

      for (Iterator<Map.Entry<Integer, Address>> it = getPositionsIterator(normalizedHash); it.hasNext();) {
         Address a = it.next().getValue();
         // if virtual nodes are enabled we have to avoid duplicate addresses
         if (!(isVirtualNodesEnabled() && owners.contains(a))) {
            if (target != null && target.equals(a))
               return null;

            owners.add(a);
            if (owners.size() >= actualReplCount)
               return owners;
         }
      }

      // might return < replCount owners if there aren't enough nodes in the list
      return owners;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultConsistentHash that = (DefaultConsistentHash) o;

      if (hashFunction != null ? !hashFunction.equals(that.hashFunction) : that.hashFunction != null) return false;
      if (numVirtualNodes != that.numVirtualNodes) return false;
      if (caches != null ? !caches.equals(that.caches) : that.caches != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = caches != null ? caches.hashCode() : 0;
      result = 31 * result + hashFunction.hashCode();
      result = 31 * result + numVirtualNodes;
      return result;
   }

   public static class Externalizer extends AbstractWheelConsistentHash.Externalizer<DefaultConsistentHash> {
      @Override
      protected DefaultConsistentHash instance() {
         return new DefaultConsistentHash();
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends DefaultConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DefaultConsistentHash>>asSet(DefaultConsistentHash.class);
      }
   }
}