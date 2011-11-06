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
package org.infinispan.distribution;

import org.infinispan.Cache;

import java.io.Serializable;
import java.util.Random;

import static org.infinispan.distribution.DistributionTestHelper.addressOf;
import static org.infinispan.distribution.DistributionTestHelper.isFirstOwner;

/**
 * A special type of key that if passed a cache in its constructor, will ensure it will always be assigned to that cache
 * (plus however many additional caches in the hash space).
 *
 * Note that this only works if all the caches have joined a single cluster before creating the key.
 * If the cluster membership changes then the keys may move to other servers.
 */
public class MagicKey implements Serializable {
   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -835275755945753954L;
   String name = null;
   int hashcode;
   String address;

   public MagicKey(Cache<?, ?> toMapTo) {
      address = addressOf(toMapTo).toString();
      Random r = new Random();
      Object dummy;
      do {
         // create a dummy object with this hashcode
         final int hc = r.nextInt();
         dummy = new Object() {
            @Override
            public int hashCode() {
               return hc;
            }
         };

      } while (!isFirstOwner(toMapTo, dummy));

      // we have found a hashcode that works!
      hashcode = dummy.hashCode();
   }

   public MagicKey(Cache<?, ?> toMapTo, String name) {
      this(toMapTo);
      this.name = name;
   }

   @Override
   public int hashCode () {
      return hashcode;
   }

   @Override
   public boolean equals (Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MagicKey magicKey = (MagicKey) o;

      if (hashcode != magicKey.hashcode) return false;
      if (address != null ? !address.equals(magicKey.address) : magicKey.address != null) return false;

      return true;
   }

   @Override
   public String toString() {
      return "MagicKey{" +
              (name == null ? "" : "name=" + name + ", ") +
              "hashcode=" + hashcode +
              ", address='" + address + '\'' +
              '}';
   }
}