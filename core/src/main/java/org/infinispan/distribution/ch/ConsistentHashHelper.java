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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.hash.Hash;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.distribution.group.GroupManagerImpl;
import org.infinispan.remoting.transport.Address;

/**
 * A helper class that handles the construction of consistent hash instances based on configuration.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ConsistentHashHelper {

   /**                                 
    * Returns a new consistent hash of the same type with the given address removed.
    *
    * @param ch       consistent hash to start with
    * @param toRemove address to remove
    * @param c        configuration
    * @return a new consistent hash instance of the same type
    */
   public static ConsistentHash removeAddress(ConsistentHash ch, Address toRemove, Configuration c) {
      if (ch instanceof UnionConsistentHash)
         return removeAddressFromUnionConsistentHash((UnionConsistentHash) ch, toRemove, c);
      else {
         ConsistentHash newCH = constructConsistentHashInstance(c);
         Set<Address> caches = new HashSet<Address>(ch.getCaches());
         caches.remove(toRemove);
         newCH.setCaches(caches);
         return newCH;
      }
   }

   private static ConsistentHash constructConsistentHashInstance(Configuration c) {
      Class<? extends ConsistentHash> chClass = Util.loadClass(c.getConsistentHashClass(), c.getClassLoader());
      Hash h = (Hash) Util.getInstance(c.getHashFunctionClass(), c.getClassLoader());
      HashSeed hs = c.getHashSeed() != null ? c.getHashSeed() :
            (HashSeed) Util.getInstance(c.getHashSeedClass(), c.getClassLoader());
      return constructConsistentHashInstance(chClass, h, hs, c.getNumVirtualNodes(), new GroupManagerImpl(c.getGroupers()));
   }

   private static ConsistentHash constructConsistentHashInstance(
            Class<? extends ConsistentHash> clazz, Hash hashFunction,
            HashSeed hashSeed, int numVirtualNodes, GroupManager groupManager) {
      ConsistentHash ch = Util.getInstance(clazz);
      if (ch instanceof AbstractWheelConsistentHash) {
         AbstractWheelConsistentHash wch = (AbstractWheelConsistentHash) ch;
         wch.setHashFunction(hashFunction);
         wch.setHashSeed(hashSeed);
         wch.setNumVirtualNodes(numVirtualNodes);
      }
      if (ch instanceof AbstractConsistentHash) {
          AbstractConsistentHash ach = (AbstractConsistentHash) ch;
          if (groupManager != null)
              ach.setGroupManager(groupManager);
      }
      return ch;
   }

   /**
    * Creates a new UnionConsistentHash instance based on the old instance, removing the provided address from both
    * target consistent hash instances in the union.
    *
    * @param uch      union consistent hash instance
    * @param toRemove address to remove
    * @param c        configuration
    * @return a new UnionConsistentHash instance
    */
   public static UnionConsistentHash removeAddressFromUnionConsistentHash(UnionConsistentHash uch, Address toRemove, Configuration c) {
      ConsistentHash newFirstCH = removeAddress(uch.getOldConsistentHash(), toRemove, c);
      ConsistentHash newSecondCH = removeAddress(uch.getNewConsistentHash(), toRemove, c);
      return new UnionConsistentHash(newFirstCH, newSecondCH);
   }

   /**
    * Creates a new consistent hash instance based on the user's configuration, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param c         configuration
    * @param addresses with which to populate the consistent hash
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Configuration c, Collection<Address> addresses) {
      ConsistentHash ch = constructConsistentHashInstance(c);
      ch.setCaches(toSet(addresses));
      return ch;
   }

   /**
    * Creates a new consistent hash instance based on the user's configuration, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param c             configuration
    * @param addresses     with which to populate the consistent hash
    *@param moreAddresses to add to the list of addresses  @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Configuration c, Collection<Address> addresses, Address... moreAddresses) {
      Set<Address> caches = new HashSet<Address>(addresses);
      caches.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(c, caches);
   }

   /**
    * Creates a new consistent hash instance based on the user's configuration, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param c             configuration
    * @param addresses     with which to populate the consistent hash
    * @param moreAddresses to add to the list of addresses
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(Configuration c, Collection<Address> addresses, Collection<Address> moreAddresses) {
      Set<Address> caches = new HashSet<Address>(addresses);
      caches.addAll(moreAddresses);
      return createConsistentHash(c, caches);
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param template An older consistent hash instance to clone
    * @param addresses with which to populate the consistent hash
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(ConsistentHash template, Collection<Address> addresses) {
      Hash hf = null;
      HashSeed hs = null;
      int numVirtualNodes = 1;
      GroupManager groupManager = null;
      if (template instanceof AbstractWheelConsistentHash) {
         AbstractWheelConsistentHash wTemplate = (AbstractWheelConsistentHash) template;
         hf = wTemplate.hashFunction;
         hs = wTemplate.hashSeed;
         numVirtualNodes = wTemplate.numVirtualNodes;
         groupManager = wTemplate.groupManager;
      }
      ConsistentHash ch = constructConsistentHashInstance(template.getClass(), hf, hs, numVirtualNodes, groupManager);
      if (addresses != null && !addresses.isEmpty())  ch.setCaches(toSet(addresses));
      return ch;
   }

   /**
    * Creates a new consistent hash instance based on the type specified, and populates the consistent hash
    * with the collection of addresses passed in.
    *
    * @param template  An older consistent hash instance to clone
    * @param addresses     with which to populate the consistent hash
    * @param moreAddresses to add to the list of addresses
    * @return a new consistent hash instance
    */
   public static ConsistentHash createConsistentHash(ConsistentHash template, Collection<Address> addresses, Address... moreAddresses) {
      Set<Address> caches = new HashSet<Address>(addresses);
      caches.addAll(Arrays.asList(moreAddresses));
      return createConsistentHash(template, caches);
   }

   private static Set<Address> toSet(Collection<Address> c) {
      if (c instanceof Set) return (Set<Address>) c;
      return new HashSet<Address>(c);
   }
}
