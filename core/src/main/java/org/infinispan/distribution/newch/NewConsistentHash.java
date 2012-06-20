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
package org.infinispan.distribution.newch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A consistent hash algorithm implementation. Implementations would typically be constructed via a
 * {@link NewConsistentHashFactory}.
 *
 * A consistent hash assigns each key a list of owners; the number of owners is defined at creation time,
 * but the consistent hash is free to return a smaller or a larger number of owners, depending on
 * circumstances, as long as each key has at least one owner.
 *
 * The first element in the list of owners is the "primary owner". A key will always have a primary owner.
 * The other owners are called "backup owners".
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Dan Berindei
 * @since 4.0
 */
public interface NewConsistentHash {

   /**
    * @return The configured number of owners for each key. Note that {code @getOwners(key)} may return
    *         a different number of owners.
    */
   int getNumOwners();

   /**
    * Should return the addresses of the nodes used to create this consistent hash.
    *
    * @return set of node addresses.
    */
   List<Address> getNodes();

   /**
    * Should be equivalent to return the first element of {@link #locateOwners}.
    * Useful as a performance optimization, as this is a frequently needed information.
    * @param key key to locate
    * @return
    */
   Address locatePrimaryOwner(Object key);

   /**
    * Finds all the owners of a key.
    *
    * @param key key to locate
    * @return A list of addresses where the key resides.
    *         Will never be {@code null}, and it will always have at least 1 element.
    */
   List<Address> locateOwners(Object key);

   /**
    * The logical equivalent of calling {@link #locateOwners} multiple times for each key in the collection of
    * keys and merging the results. Implementations may be optimised for such a bulk lookup.
    *
    *
    * @param keys keys to locate.
    * @return list of nodes that own at least one of the keys.
    */
   Collection<Address> locateAllOwners(Collection<Object> keys);

   /**
    * Test to see whether a key is owned by a given node.
    *
    * @param nodeAddress address of the node to test
    * @param key key to test
    * @return {@code true} if the key is mapped to the address; {@code false} otherwise
    */
   boolean isKeyLocalToNode(Address nodeAddress, Object key);

}
