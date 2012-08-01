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

package org.infinispan.distribution.ch;

import java.util.List;
import java.util.Set;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

/**
 * This interface gives access to some implementation details of the consistent hash.
 *
 * Our consistent hashes work by splitting the hash space (the set of possible hash codes) into
 * fixed segments and then assigning those segments to nodes dynamically. The number of segments
 * is defined at creation time, and the mapping of keys to segments never changes.
 * The mapping of segments to nodes does change as the membership of the cache changes.
 *
 * Normally application code doesn't need to know about this implementation detail, but some
 * applications may benefit from the knowledge that all the keys that map to one segment are
 * always located on the same server.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public interface AdvancedConsistentHash extends ConsistentHash {

   /**
    * @return The actual number of hash space segments. Note that it may not be the same as the number
    *         of segments passed in at creation time.
    */
   int getNumSegments();

   /**
    * @return The hash space segment that a key maps to.
    */
   int getSegment(Object key);

   /**
    * @return All the nodes that own a given hash space segment. The returned list is a copy of the internal list.
    */
   List<Address> locateOwnersForSegment(int segmentId);

   /**
    * @return The primary owner of a given hash space segment. This is equivalent to {@code locateOwnersForSegment(segmentId).get(0)} but is more efficient
    */
   Address locatePrimaryOwnerForSegment(int segmentId);

   Set<Integer> getSegmentsForOwner(Address owner);
}
