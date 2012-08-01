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
package org.infinispan.distribution.oldch;

import org.infinispan.CacheException;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;
import org.infinispan.util.Util;

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
   public List<Integer> getHashIds(Address a) {
      throw new UnsupportedOperationException("Unsupported!");
   }

   public ConsistentHash getNewConsistentHash() {
      return newCH;
   }

   public ConsistentHash getOldConsistentHash() {
      return oldCH;
   }

   public static class Externalizer extends AbstractExternalizer<UnionConsistentHash> {
      @Override
      public void writeObject(ObjectOutput output, UnionConsistentHash uch) throws IOException {
         output.writeObject(uch.oldCH);
         output.writeObject(uch.newCH);
      }

      @Override
      public UnionConsistentHash readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnionConsistentHash((ConsistentHash) input.readObject(), (ConsistentHash) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.UNION_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends UnionConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends UnionConsistentHash>>asSet(UnionConsistentHash.class);
      }
   }
}
