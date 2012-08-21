/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.statetransfer;

import org.infinispan.distribution.ch.ConsistentHash;

import java.io.Serializable;
import java.util.Random;

/**
 * A key that maps to a given data segment of the ConsistentHash.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
final class TestKey implements Serializable {

   private static final long serialVersionUID = -42;

   /**
    * A name used for easier debugging. This is not relevant for equals() and hashCode().
    */
   private final String name;

   /**
    * A carefully crafted hash code.
    */
   private final int hashCode;

   public TestKey(String name, int segmentId, ConsistentHash ch) {
      this.name = name;

      Random rnd = new Random();
      Integer r;
      do {
         r = rnd.nextInt();
      } while (segmentId != ch.getSegment(r));

      hashCode = r;
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != TestKey.class) return false;
      TestKey other = (TestKey) o;
      return hashCode == other.hashCode;
   }

   @Override
   public String toString() {
      return "TestKey{name=" + name + ", hashCode=" + hashCode + '}';
   }
}