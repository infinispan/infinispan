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

import org.infinispan.util.hash.Hash;
import org.infinispan.util.hash.MurmurHash2;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Version one consistent hash function based on {@link org.infinispan.util.hash.MurmurHash2};
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashV1 implements ConsistentHash {

   private static final Log log = LogFactory.getLog(ConsistentHashV1.class);
   
   private final SortedMap<Integer, InetSocketAddress> positions = new TreeMap<Integer, InetSocketAddress>();

   private int hashSpace;

   Hash mmh = new MurmurHash2();

   @Override
   public void init(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, int hashSpace) {
      for (InetSocketAddress addr : servers2HashCode.keySet()) {
         positions.put(servers2HashCode.get(addr), addr);
      }
      if (log.isTraceEnabled())
         log.tracef("Positions are: %s", positions);
      this.hashSpace = hashSpace;
   }

   @Override
   public InetSocketAddress getServer(byte[] key) {
      int keyHashCode = mmh.hash(key);
      if (keyHashCode == Integer.MIN_VALUE) keyHashCode += 1;
      int hash = Math.abs(keyHashCode);

      SortedMap<Integer, InetSocketAddress> candidates = positions.tailMap(hash % hashSpace);
      if (log.isTraceEnabled()) {
         log.tracef("Found possible candidates: %s", candidates);
      }
      if (candidates.isEmpty()) {
         InetSocketAddress socketAddress = positions.get(positions.firstKey());
         if (log.isTraceEnabled()) {
            log.tracef("Over the wheel, returning first member: %s", socketAddress);
         }
         return socketAddress;
      } else {
         InetSocketAddress socketAddress = candidates.get(candidates.firstKey());
         if (log.isTraceEnabled()) {
            log.tracef("Found candidate: %s", socketAddress);
         }
         return socketAddress;
      }
   }
}
