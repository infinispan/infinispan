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
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import static java.lang.String.format;

/**
 * <p>
 * Abstract class for the wheel-based CH implementations.
 * </p>
 * 
 * <p>
 * This base class supports virtual nodes. To enable virtual nodes you must set
 * <code>numVirtualNodes</code> to a number &gt; 1.
 * </p> 
 *
 * <p>
 * Enabling virtual nodes means that a cache will appear multiple times on the hash
 * wheel. If an implementation doesn't want to support this, it should override
 * {@link #setNumVirtualNodes(Integer)} to throw an {@link IllegalArgumentException}
 * for values != 1.
 * </p>
 *
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.2
 */
public abstract class AbstractWheelConsistentHash extends AbstractConsistentHash {

   protected final Log log;
   protected final boolean trace;

   protected Hash hashFunction;
   protected int numVirtualNodes = 1;

   protected Set<Address> caches;
   // A map of normalized hashes -> cache addresses, represented as two arrays for performance considerations
   // positionKeys.length == positionValues.length == caches.size() * numVirtualNodes
   // positionKeys is sorted so we can search in it using binary search, see getPositionIndex(int)
   protected int[] positionKeys;
   protected Address[] positionValues;

   protected AbstractWheelConsistentHash() {
      log = LogFactory.getLog(getClass());
      trace = log.isTraceEnabled();
   }

   public void setHashFunction(Hash h) {
      checkCachesUninitialized("hash function");
      hashFunction = h;
   }

   private void checkCachesUninitialized(String property) {
      if (caches != null) {
         throw new IllegalStateException(format(
               "Must configure the %s before adding the caches", property));
      }
   }

   public void setNumVirtualNodes(Integer numVirtualNodes) {
      checkCachesUninitialized("number of virtual nodes");
      this.numVirtualNodes = numVirtualNodes;
   }

   @Override
   public void setCaches(Set<Address> newCaches) {
      if (newCaches.size() == 0 || newCaches.contains(null))
         throw new IllegalArgumentException("Invalid cache list for consistent hash: " + newCaches);

      if (((long) newCaches.size()) * numVirtualNodes > Integer.MAX_VALUE)
         throw new IllegalArgumentException("Too many nodes: " + newCaches.size() + " * " + numVirtualNodes
                                                  + " exceeds the available hash space");

      // first find the correct position key for each node, as it may be different from its normalized hash
      // still, we would like the cache address to map to that cache as much as possible
      // so we add the virtual nodes (if any) only after we have added all the "real" nodes
      TreeMap<Integer, Address> positions = new TreeMap<Integer, Address>();
      for (Address a : newCaches) {
         addNode(positions, a, getNormalizedHash(a));
      }

      if (isVirtualNodesEnabled()) {
         for (Address a : newCaches) {
            for (int i = 1; i < numVirtualNodes; i++) {
               // we get the normalized hash from the VirtualAddress, but we store the real address in the positions map
               Address va = new VirtualAddress(a, i);
               addNode(positions, a, getNormalizedHash(va));
            }
         }
      }

      log.debugf("Using %d virtualNodes to initialize consistent hash wheel ", numVirtualNodes);
      log.tracef("Positions are: %s", positions);

      // then populate caches, positionKeys and positionValues with the correct values (and in the correct order)
      caches = new LinkedHashSet<Address>(newCaches.size());
      positionKeys = new int[positions.size()];
      positionValues = new Address[positions.size()];
      int i = 0;
      for (Map.Entry<Integer, Address> position : positions.entrySet()) {
         caches.add(position.getValue());
         positionKeys[i] = position.getKey();
         positionValues[i] = position.getValue();
         i++;
      }
      log.tracef("Consistent hash initialized: %s", this);
   }

   private void addNode(TreeMap<Integer, Address> positions, Address a, int positionIndex) {
      // this is deterministic since the address list is ordered and the order is consistent across the grid
      while (positions.containsKey(positionIndex)) {
         if (positionIndex == Integer.MAX_VALUE)
            positionIndex = 0;
         else
            positionIndex = positionIndex + 1;
      }
      positions.put(positionIndex, a);
   }

   @Override
   public final Set<Address> getCaches() {
      return caches;
   }

   protected final int getPositionIndex(int normalizedHash) {
      int index = Arrays.binarySearch(positionKeys, normalizedHash);
      // Arrays.binarySearch returns (-(insertion point) - 1) when the value is not found
      // we need (insertion point) instead
      if (index < 0) {
         index = -index - 1;
         if (index == positionKeys.length)
            index = 0;
      }

      return index;
   }

   /**
    * Creates an iterator over the positions "map" starting at the index specified by the <code>normalizedHash</code>.
    */
   protected final Iterator<Address> getPositionsIterator(final int normalizedHash) {
      final int startIndex = getPositionIndex(normalizedHash);
      return new Iterator<Address>() {
         int i = startIndex;

         @Override
         public boolean hasNext() {
            return i >= 0;
         }

         @Override
         public Address next() {
            Address value = positionValues[i];
            i++;
            // go back to the start
            if (i == positionKeys.length)
               i = 0;
            // we have come full cycle
            if (i == startIndex)
               i = -1;
            return value;
         }

         @Override
         public void remove() {
            throw new UnsupportedOperationException("The positions map cannot be modified");
         }
      };
   }

   @Override
   public final List<Integer> getHashIds(Address a) {
      // Not the most efficient way of doing this but it's usage it's so far
      // limited to the HotRod server and it does it only on once on startup,
      // so there's no urgency in finding a better way to implement this.

      // If virtual nodes are enabled, the list should be as long as
      // the number of virtual nodes, otherwise it's only one element.
      List<Integer> hashIds = null;
      boolean vNodesEnabled = isVirtualNodesEnabled();

      for (int i = 0; i < positionValues.length; i++) {
         if (positionValues[i].equals(a)) {
            if (vNodesEnabled && hashIds == null)
               hashIds = new ArrayList<Integer>(numVirtualNodes);

            if (vNodesEnabled)
               hashIds.add(positionKeys[i]);
            else
               return Collections.singletonList(positionKeys[i]);
         }
      }

      if (hashIds == null)
         return Collections.emptyList();
      else
         return hashIds;
   }

   public final int getNormalizedHash(final Object key) {
      return Util.getNormalizedHash(key, hashFunction);
   }

   public final boolean isVirtualNodesEnabled() {
      return numVirtualNodes > 1;
   }


   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder(getClass().getSimpleName());
      sb.append(" {");
      for (int i = 0; i < positionKeys.length; i++) {
         if (i > 0) {
            sb.append(", ");
         }
         sb.append(positionKeys[i]).append(": ").append(positionValues[i]);
      }
      sb.append("}");
      return sb.toString();
   }

   @Override
   public final Address primaryLocation(final Object key) {
      final int normalizedHash = getNormalizedHash(getGrouping(key));
      return positionValues[getPositionIndex(normalizedHash)];
   }


   public static abstract class Externalizer<T extends AbstractWheelConsistentHash> extends AbstractExternalizer<T> {

      // Injecting a classloader here is redundant and complicates marshalling
      // code. Let JBoss Marshalling's class resolver do its job, which is
      // resolving classes sent around.

      protected abstract T instance();

      @Override
      public void writeObject(ObjectOutput output, T abstractWheelConsistentHash) throws IOException {
         output.writeInt(abstractWheelConsistentHash.numVirtualNodes);
         output.writeObject(abstractWheelConsistentHash.hashFunction);
         output.writeObject(abstractWheelConsistentHash.caches);
      }

      @Override
      @SuppressWarnings("unchecked")
      public T readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         T instance = instance();
         instance.numVirtualNodes = unmarshaller.readInt();
         Hash hash = (Hash) unmarshaller.readObject();
         instance.setHashFunction(hash);
         Set<Address> caches = (Set<Address>) unmarshaller.readObject();
         instance.setCaches(caches);
         return instance;
      }
   }
}

