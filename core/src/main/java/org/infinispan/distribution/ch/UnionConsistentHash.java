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

import org.infinispan.CacheException;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A delegating wrapper that locates keys by getting a union of locations reported by two other ConsistentHash
 * implementations it delegates to.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = UnionConsistentHash.Externalizer.class, id = Ids.UNION_CONSISTENT_HASH)
public class UnionConsistentHash extends AbstractConsistentHash {

   ConsistentHash oldCH, newCH;

   public UnionConsistentHash(ConsistentHash oldCH, ConsistentHash newCH) {
      if ((oldCH instanceof UnionConsistentHash) || (newCH instanceof UnionConsistentHash))
         throw new CacheException("Expecting both newCH and oldCH to not be Unions!!  oldCH=[" + oldCH.getClass() + "] and newCH=[" + newCH.getClass() + "]");
      this.oldCH = oldCH;
      this.newCH = newCH;
   }

   @Override
   public void setCaches(Set<Address> caches) {
      // no op
   }

   @Override
   public Set<Address> getCaches() {
      return Collections.emptySet();
   }

   @Override
   public List<Address> locate(Object key, int replCount) {
      Set<Address> addresses = new LinkedHashSet<Address>();
      addresses.addAll(oldCH.locate(key, replCount));
      addresses.addAll(newCH.locate(key, replCount));
      return Immutables.immutableListConvert(addresses);
   }

   @Override
   public int getHashId(Address a) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   public List<Address> getStateProvidersOnLeave(Address leaver, int replCount) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   @Override
   public List<Address> getStateProvidersOnJoin(Address joiner, int replCount) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   @Override
   public List<Address> getBackupsForNode(Address node, int replCount) {
      return oldCH.locate(node, replCount);
   }

   @Override
   public int getHashSpace() {
      int oldHashSpace = oldCH.getHashSpace();
      int newHashSpace = newCH.getHashSpace();
      // In a union, the hash space is the biggest of the hash spaces.
      return oldHashSpace > newHashSpace ? oldHashSpace : newHashSpace;
   }

   public ConsistentHash getNewConsistentHash() {
      return newCH;
   }

   public ConsistentHash getOldConsistentHash() {
      return oldCH;
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {

      public void writeObject(ObjectOutput output, Object object) throws IOException {
         UnionConsistentHash uch = (UnionConsistentHash) object;
         output.writeObject(uch.oldCH);
         output.writeObject(uch.newCH);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnionConsistentHash((ConsistentHash) input.readObject(), (ConsistentHash) input.readObject());
      }
   }
}
