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

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Special implementation of {@link ConsistentHash} for replicated caches.
 * The hash-space has only one segment owned by all members.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ReplicatedConsistentHash implements ConsistentHash {

   private final List<Address> members;

   private static final Set<Integer> theSegment = Collections.singleton(0);

   public ReplicatedConsistentHash(List<Address> members) {
      this.members = new ArrayList<Address>(members);
   }

   @Override
   public int getNumSegments() {
      return 1;
   }

   @Override
   public int getNumOwners() {
      return members.size();
   }

   @Override
   public List<Address> getMembers() {
      return members;
   }

   @Override
   public Hash getHashFunction() {
      return null;
   }

   @Override
   public int getSegment(Object key) {
      return 0;
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return members;
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return members.get(0);
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return theSegment;
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      return members.get(0);
   }

   @Override
   public List<Address> locateOwners(Object key) {
      return members;
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      return new HashSet<Address>(members);
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return true;
   }

   @Override
   public String toString() {
      return "ReplicatedConsistentHash{" +
            "members=" + members +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<ReplicatedConsistentHash> {

      @Override
      public void writeObject(ObjectOutput output, ReplicatedConsistentHash ch) throws IOException {
         output.writeObject(ch.members);
      }

      @Override
      @SuppressWarnings("unchecked")
      public ReplicatedConsistentHash readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         List<Address> members = (List<Address>) unmarshaller.readObject();
         return new ReplicatedConsistentHash(members);
      }

      @Override
      public Integer getId() {
         return Ids.REPLICATED_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends ReplicatedConsistentHash>> getTypeClasses() {
         return Collections.<Class<? extends ReplicatedConsistentHash>>singleton(ReplicatedConsistentHash.class);
      }
   }
}
