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

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.min;

/**
 * <a href = "http://weblogs.java.net/blog/tomwhite/archive/2007/11/consistent_hash.html">Consistent hashing
 * algorithm</a>.  Each target is entered into the pool <i>weight[i]</i>*<i>weightFactor</i> times. Where
 * <i>weight[i]</i> and <i>weightFactor</i> are integers greater than zero.
 * <p/>
 * Based on akluge's impl on <a href-="http://www.vizitsolutions.com/ConsistentHashingCaching.html">http://www.vizitsolutions.com/ConsistentHashingCaching.html</a>
 *
 * @author akluge
 * @author Manik Surtani
 */
public class ExperimentalDefaultConsistentHash extends AbstractConsistentHash {
   /**
    * A Weight and weight factor of 1 gives one node per address.  In future we may decide to make these configurable,
    * to allow for virtual nodes and a better spread of state across nodes, but we need to ensure we deal with backups
    * not falling on virtual nodes on the same cache instances first.
    */
   private static final int DEFAULT_WEIGHT = 1;
   private static final int DEFAULT_WEIGHTFACTOR = 1;

   private List<Address> nodes;
   private List<Entry> pool;
   private int poolSize;

   public static class Externalizer extends AbstractExternalizer<ExperimentalDefaultConsistentHash> {
      @Override
      public void writeObject(ObjectOutput output, ExperimentalDefaultConsistentHash object) throws IOException {
         output.writeObject(object.nodes);
      }

      @Override
      @SuppressWarnings("unchecked")
      public ExperimentalDefaultConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         List<Address> addresses = (List<Address>) input.readObject();
         ExperimentalDefaultConsistentHash gch = new ExperimentalDefaultConsistentHash();
         gch.setCaches(addresses);
         return gch;
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends ExperimentalDefaultConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends ExperimentalDefaultConsistentHash>>asSet(ExperimentalDefaultConsistentHash.class);
      }
   }

   @Override
   public Set<Address> getCaches() {
      return new LinkedHashSet<Address>(nodes);
   }

   @Override
   public void setCaches(Set<Address> caches) {
      setCaches((Collection<Address>)caches);
   }

   public void setCaches(Collection<Address> caches) {
      nodes = new ArrayList<Address>(caches);
      int numNodes = nodes.size();

      int poolSize = 0;

      for (int i = 0; i < numNodes; i++) {
         poolSize += DEFAULT_WEIGHT * DEFAULT_WEIGHTFACTOR;
      }
      this.poolSize = poolSize;
      pool = new ArrayList<Entry>(poolSize);

      int numEntries = 0;
      for (int i = 0; i < numNodes; i++) {
         numEntries = add(nodes.get(i), DEFAULT_WEIGHT * DEFAULT_WEIGHTFACTOR, numEntries);
      }
      Collections.sort(pool);
      nodes = getSortedCachesList();
   }

   private List<Address> getSortedCachesList() {
      ArrayList<Address> caches = new ArrayList<Address>();
      for (Entry e : pool) {
         if (!caches.contains(e.address)) caches.add(e.address);
      }
      caches.trimToSize();
      return caches;
   }

   /**
    * Adds an Address to the pool of available addresses.
    *
    * @param node     Address to be added to the hash pool.
    * @param count    An int giving the number of times the node is added to the pool. The count is greater than zero,
    *                 and is likely greater than 10 if weights are used. The count for the i-th node is usually the
    *                 <i>weights</i>[i]*<i>weightfactor</i>, if weights are used.
    * @param position The position in the pool to begin adding node entries.
    * @return position;
    */
   private int add(Address node, int count, int position) {
      int hash;
      String nodeName = node.toString();
      for (int i = 0; i < count; i++) {
         hash = hash((Integer.toString(i) + nodeName).getBytes(Charset.forName("UTF-8")));
         pool.add(position++, new Entry(node, nodeName, i, hash));
      }
      return position;
   }

   /**
    * The distance between the first entries in the address array for two caches, a1 and a2. Of questionable use when
    * virtual nodes are employed.
    *
    * @param a1 The address of the first cache.
    * @param a2 The address of the second cache.
    * @return Am int containing the difference between these two indices.
    */
   public int getDistance(Address a1, Address a2) {
      if (a1 == null || a2 == null) throw new NullPointerException("Cannot find the distance between null servers.");

      int p1 = nodes.indexOf(a1);
      if (p1 < 0)
         throw new IllegalArgumentException("Address " + a1 + " not in the addresses list of this consistent hash impl!");

      int p2 = nodes.indexOf(a2);
      if (p2 < 0)
         throw new IllegalArgumentException("Address " + a2 + " not in the addresses list of this consistent hash impl!");

      if (p1 <= p2)
         return p2 - p1;
      else
         return pool.size() - (p1 - p2);
   }

   /**
    * Two hashes are adjacent if they are next to each other in the consistent hash.
    *
    * @param a1 The address of the first cache.
    * @param a2 The address of the second cache.
    * @return A boolean, true if they are adjacent, false if not.
    */
   public boolean isAdjacent(Address a1, Address a2) {
      int distance = getDistance(a1, a2);
      return distance == 1 || distance == pool.size() - 1;
   }

   @Override
   public List<Address> locate(Object key, int replCount) {
      if (key == null) throw new NullPointerException("Attempt to get with null key");

      int clusterSize = pool.size();
      int numCopiesToFind = min(replCount, clusterSize);
      int hashValue = hash(key);
      return locate(hashValue, numCopiesToFind, replCount);
   }

   /**
    * Returns a List of <i>numCopiesToFind</i> unique Addresses.
    *
    * @param hashValue       An int, usually a hash, to be mapped to a bin via the CH.
    * @param numCopiesToFind number of copies to find
    * @param replCount       replication count
    * @return Returns a List of <i>numCopiesToFind</i> unique Addresses.
    */
   private List<Address> locate(int hashValue, int numCopiesToFind, int replCount) {
      // Stop looking if we have checked the entire pool.
      int checked = 0;
      // Start looking at the first (primary) node for entries for this value.
      int inode = findNearestNodeInPool(hashValue);
      List<Address> nodes = new ArrayList<Address>(numCopiesToFind);

      while (nodes.size() < replCount && checked < poolSize) {
         Entry poolEntry;
         if ((poolEntry = pool.get(inode)) != null && nodes.indexOf(poolEntry.address) < 0) {
            nodes.add(poolEntry.address);
         }
         inode = (++inode) % poolSize;
         checked++;
      }
      return nodes;
   }

   /**
    * Find a target for a hash key within the pool of node Entries. We search within a slice of the array bounded by
    * lowerBound and upperBound. Further we assume that lowerBound and upperBound are small enough that their sum will
    * not overflow an int.
    * <p/>
    *
    * @param hash The desired hash to locate.
    * @return An int giving the index of the desired entry in the list of targets. If the target is not found, then
    *         -(lowerBound +1) will be returned, where lowerBound is the lower bound of the search after possibly
    *         several iterations.
    */
   private int binarySearch(int hash) {
      int lowerBound = 0;
      int upperBound = pool.size() - 1;
      while (lowerBound <= upperBound) {
         // Fast div by 2. We assume that the number of targets is small enough
         // that the sum will not overflow an int.
         int mid = (lowerBound + upperBound) >>> 1;
         int currentHash = pool.get(mid).hash;

         if (currentHash < hash) {
            lowerBound = mid + 1;
         } else if (currentHash > hash) {
            upperBound = mid - 1;
         } else {
            return mid;
         }
      }
      // The +1 ensures that the return value is negative, even when the hash
      // is off the left edge of the array.
      return -(lowerBound + 1);
   }

   /**
    * Finds the lowest index into the pool ArrayList such that the hash of the i-th entry >= hash.
    *
    * @param hash The hash being mapped to a bin via the consistent hash.
    * @return An int, the lowest index into the target array such that the hash of the i-th entry >= hash.
    */
   private int findNearestNodeInPool(int hash) {
      // Find the index of the node - or at least a near one.
      // We only search up to targets.length-1. If the element
      // is greater than the last entry in the list, then map
      // it to the first one.
      int nodeIndex = binarySearch(hash);

      // If the returned value is less than zero, then no exact match was found.
      if (nodeIndex < 0) {
         // The value returned is -(lowerBound +1), we want the lower bound back.
         nodeIndex = -(nodeIndex + 1);

         // If hash is greater than the last entry, wrap around to the first.
         if (nodeIndex >= pool.size()) {
            nodeIndex = 0;
         }
      }

      return nodeIndex;
   }

   /**
    * Use the objects built in hash to obtain an initial value, then use a second four byte hash to obtain a more
    * uniform distribution of hash values. This uses a <a href = "http://burtleburtle.net/bob/hash/integer.html">4-byte
    * (integer) hash</a>, which produces well distributed values even when the original hash produces thghtly clustered
    * values.
    * <p/>
    * It is important that the object implement its own hashcode, and not use the Object hashcode.
    *
    * @param object object to hash
    * @return an appropriately spread hash code
    */
   private int hash(Object object) {
      int hash = object.hashCode();

      hash = (hash + 0x7ED55D16) + (hash << 12);
      hash = (hash ^ 0xc761c23c) ^ (hash >> 19);
      hash = (hash + 0x165667b1) + (hash << 5);
      hash = (hash + 0xd3a2646c) ^ (hash << 9);
      hash = (hash + 0xfd7046c5) + (hash << 3);
      hash = (hash ^ 0xb55a4f09) ^ (hash >> 16);

      return hash;
   }

   @Override
   public List<Integer> getHashIds(Address a) {
      throw new RuntimeException("Not yet implemented");
   }

   /**
    * @return A String representing the object pool.
    */
   @Override
   public String toString() {
      return " pool: " + pool;
   }

   @Override
   public boolean equals(Object other) {
      if (other == null
            || !(other instanceof ExperimentalDefaultConsistentHash)) {
         return false;
      }

      ExperimentalDefaultConsistentHash otherHash = (ExperimentalDefaultConsistentHash) other;
      return Util.safeEquals(pool, otherHash.pool);
   }

   @Override
   public int hashCode() {
      int hashCode = 1;
      for (Entry e : pool) hashCode = 31 * hashCode + e.hash;
      return hashCode;
   }

   /**
    * An entry into a consistent hash. It wraps the original object, the object's hash as used to generate the
    * consistent hash, the value extracted from the object used to generate the hash, and the modifier used to
    * differentiate the hash.
    */
   public static class Entry implements Comparable<Entry> {
      public final int differentiator;
      public final int hash;
      public final Address address;
      public final String string;

      public Entry(Address address, String string, int differentiator, int hash) {
         this.differentiator = differentiator;
         this.hash = hash;
         this.address = address;
         this.string = string;
      }

      /**
       * Compare this Entry with another Entry. First the hash values are compared, then the differentiator is compared.
       * if the hash values are equal.
       *
       * @param other An Entry object to be compared with this object. Returns <ul> <li>-1 if this Entry is less than
       *              the other Entry.</li> <li>0  if they are equal.</li> <li>+1 if this Entry is greater than the
       *              other Entry.</li> </ul>
       * @return
       */
      @Override
      public int compareTo(Entry other) {
         if (this.hash < other.hash) {
            return -1;
         }

         if (this.hash > other.hash) {
            return 1;
         }

         if (this.differentiator < other.differentiator) {
            return -1;
         }

         if (this.differentiator > other.differentiator) {
            return +1;
         }

         return 0;
      }

      @Override
      public boolean equals(Object other) {
         if (other instanceof Entry) {
            Entry otherEntry = (Entry) other;

            return hash == otherEntry.hash
                  && differentiator == otherEntry.differentiator
                  && address.equals(otherEntry.address);
         }
         return false;
      }

      @Override
      public int hashCode() {
         return hash;
      }


      @Override
      public String toString() {
         return string + ":" + Integer.toHexString(hash);
      }
   }
}