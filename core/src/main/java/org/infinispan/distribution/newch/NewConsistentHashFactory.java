/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.newch;

import java.util.List;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

/**
 * Factory for {@link NewConsistentHash} instances.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public interface NewConsistentHashFactory {

   /**
    * Check if the ownership of keys would be properly balanced in the {@code existingCH}
    * consistent hash, if the list of members was changed to {@code newMembers}.
    *
    * <p>This method will almost always return {@code true} if some members left or joined.
    * However, an implementation may decide to perform rebalancing in multiple steps, so
    * it may return {@true} even if the members stay the same. A client should call
    * {@code createConsistentHash()} in a loop until {@code needNewConsistentHash()}
    * returns false to ensure that the keys are properly balanced.
    *
    * @param existingCH An existing consistent hash instance, may be {@code null}.
    *               If null, the method will return {@code true}.
    * @param newMembers A list of addresses representing the new cache members.
    * @return {@code true} if a new consistent hash should be created.
    */
   boolean needNewConsistentHash(NewConsistentHash existingCH, List<Address> newMembers);

   /**
    * Create a new consistent hash instance.
    *
    * @param hashFunction The hash function to use on top of the keys' own {@code hashCode()} implementation.
    * @param numOwners The ideal number of owners for each key. The created consistent hash
    *                  can have more or less owners, but each key will have at least one owner.
    * @param numSegments Number of hash wheel segments. The implementation may round up the number
    *                    of segments for performance, or may ignore the parameter altogether.
    * @param members A list of addresses representing the new cache members.
    */
   NewConsistentHash createConsistentHash(Hash hashFunction, int numOwners, int numSegments, List<Address> members);

   /**
    * Create a new consistent hash instance. The new instance will have the same number of owners
    * and number of segments as the base CH.
    *
    * @param baseCH An existing consistent hash instance, may be {@code null}.
    *               If non-null, the factory will do its best to maintain existing ownership.
    * @param newMembers A list of addresses representing the new cache members.
    */
   NewConsistentHash createConsistentHash(NewConsistentHash baseCH, List<Address> newMembers);
}
