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
package org.infinispan.client.hotrod.impl.consistenthash;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.infinispan.util.hash.Hash;
import org.infinispan.util.hash.MurmurHash2;
import org.infinispan.util.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;

/**
 * Version one consistent hash function based on {@link org.infinispan.util.hash.MurmurHash2};
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashV1 implements ConsistentHash {

   private static final BasicLogger log = BasicLogFactory.getLog(ConsistentHashV1.class);
   
   private final SortedMap<Integer, SocketAddress> positions = new TreeMap<Integer, SocketAddress>();

   private int hashSpace;

   protected Hash hash = new MurmurHash2();

   private int numKeyOwners;

   private Random rnd = new Random();

   @Override
   public void init(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, int hashSpace) {
      for (Map.Entry<SocketAddress, Set<Integer>> entry : servers2Hash.entrySet()){
         SocketAddress addr = entry.getKey();
         for (Integer hash : entry.getValue()) {
            SocketAddress prev = positions.put(hash, addr);
            if (prev != null)
               log.debugf("Adding hash (%d) again, this time for %s. Previously it was associated with: %s", hash, addr, prev);
         }
      }

      log.tracef("Positions (%d entries) are: %s", positions.size(), positions);
      this.hashSpace = hashSpace;
      this.numKeyOwners = numKeyOwners;
   }

   @Override
   public SocketAddress getServer(byte[] key) {
      int keyHashCode = getNormalizedHash(key);
      if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
      int hash = Math.abs(keyHashCode);

      SortedMap<Integer, SocketAddress> candidates = positions.tailMap(hash % hashSpace);
      if (log.isTraceEnabled()) {
         log.tracef("Found possible candidates: %s", candidates);
      }
      int index = getIndex();
      if (candidates.size() <= index) {
         int newIndex = index - candidates.size();
         SocketAddress socketAddress = getItemAtPosition(newIndex, positions);
         if (log.isTraceEnabled()) {
            log.tracef("Over the wheel, returning member: %s", socketAddress);
         }
         return socketAddress;
      } else {
         SocketAddress socketAddress = getItemAtPosition(index, candidates);
         if (log.isTraceEnabled()) {
            log.tracef("Found candidate: %s", socketAddress);
         }
         return socketAddress;
      }
   }

   private int getIndex() {
      return rnd.nextInt(Math.min(numKeyOwners, positions.size()));
   }

   private SocketAddress getItemAtPosition(int position, SortedMap<Integer, SocketAddress> map) {
      Iterator<Map.Entry<Integer,SocketAddress>> iterator = map.entrySet().iterator();
      for (int i = 0; i < position; i++) {
         iterator.next();
      }
      return iterator.next().getValue();
   }

   public void setHash(Hash hash) {
      this.hash = hash;
   }

   @Override
   public int getNormalizedHash(Object key) {
      return hash.hash(key) & Integer.MAX_VALUE; // make sure no negative numbers are involved.
   }

}
