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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A consistent hash algorithm implementation.  Implementations would typically be constructed via reflection so should
 * implement a public, no-arg constructor.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface ConsistentHash {

   /**
    * Sets the collection of cache addresses in the cluster.  The implementation should store these internally and use
    * these to locate keys.
    *
    * @param caches A set of unique caches in cluster.
    */
   void setCaches(Set<Address> caches);

   /**
    * Should return a collection of cache addresses in the cluster.
    *
    * @return set of unique of cache addresses
    */
   Set<Address> getCaches();

   /**
    * Locates a key, given a replication count (number of copies).
    *
    * @param key       key to locate
    * @param replCount replication count (number of copies)
    * @return a list of addresses where the key resides, where this list is a subset of the addresses set in {@link
    *         #setCaches(java.util.Set)}.  Should never be null, and should contain replCount elements or the max
    *         number of caches available, whichever is smaller.
    */
   List<Address> locate(Object key, int replCount);

   /**
    * The logical equivalent of calling {@link #locate(Object, int)} multiple times for each key in the collection of
    * keys. Implementations may be optimised for such a bulk lookup, or may just repeatedly call {@link #locate(Object,
    * int)}.
    *
    * @param keys      keys to locate
    * @param replCount replication count (number of copies) for each key
    * @return Map of locations, keyed on key.
    */
   Map<Object, List<Address>> locateAll(Collection<Object> keys, int replCount);

   /**
    * Test to see whether a key is mapped to a given address.
    * @param a address to test
    * @param key key to test
    * @param replCount repl count
    * @return true if the key is mapped to the address; false otherwise
    */
   boolean isKeyLocalToAddress(Address a, Object key, int replCount);

   /**
    * Returns a list of values between 0 and the hash space limit, or hash id,
    * for a particular address. If virtual nodes are disabled, the list will
    * only contain a single element, whereas if virtual nodes are enabled, this
    * list's size will be the number of virtual nodes configured. If there are
    * no hash ids for that address, it returns an empty list.
    *
    * @return A list of N size where N is the configured number of virtual
    *         nodes, or an empty list if there're no hash ids associated with
    *         the address.
    */
   List<Integer> getHashIds(Address a);

   /**
    * Returns the nodes that need will replicate their state if the specified node crashes. The return collection
    * should contain all the nodes that backup-ed on leaver and one of the nodes which acted as a backup for the leaver .
    * <p>
    * Pre: leaver must be present in the caches known to this CH, as returned by {@link #getCaches()}
    * @param leaver the node that leaves the cluster
    * @param replCount
    * @deprecated No longer supported. This method doesn't make sense with virtual nodes enabled.
    */
   @Deprecated
   List<Address> getStateProvidersOnLeave(Address leaver, int replCount);

   /**
    * Returns the nodes that would act as state providers when a new node joins:
    * - the nodes for which the joiner is a backup
    * - the nodes that held joiner's state
    * @deprecated No longer supported. This method doesn't make sense with virtual nodes enabled.
    */
   @Deprecated
   List<Address> getStateProvidersOnJoin(Address joiner, int replCount);

   /**
    * Returns the nodes that backup data for the supplied node including the node itself.
    * @deprecated No longer supported. This method doesn't make sense with virtual nodes enabled.
    */
   @Deprecated
   List<Address> getBackupsForNode(Address node, int replCount);

   /**
    * Should be equivalent to return the first element of {@link #locate(Object, int)}.
    * Useful as a performance optimization, as this is a frequently needed information.
    * @param key key to locate
    * @return
    */
   Address primaryLocation(Object key);
}
