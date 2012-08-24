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

package org.infinispan.topology;

import java.io.Serializable;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHashFactory;

/**
 * This class contains the information that a cache needs to supply to the coordinator when starting up.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheJoinInfo implements Serializable {
   private final ConsistentHashFactory consistentHashFactory;
   private final Hash hashFunction;
   private final int numSegments;
   private final int numOwners;
   private final long timeout;
   private final int viewId;

   public CacheJoinInfo(ConsistentHashFactory consistentHashFactory, Hash hashFunction, int numSegments,
                        int numOwners, long timeout, int viewId) {
      this.consistentHashFactory = consistentHashFactory;
      this.hashFunction = hashFunction;
      this.numSegments = numSegments;
      this.numOwners = numOwners;
      this.timeout = timeout;
      this.viewId = viewId;
   }

   public ConsistentHashFactory getConsistentHashFactory() {
      return consistentHashFactory;
   }

   public Hash getHashFunction() {
      return hashFunction;
   }

   public int getNumSegments() {
      return numSegments;
   }

   public int getNumOwners() {
      return numOwners;
   }

   public long getTimeout() {
      return timeout;
   }

   public int getViewId() {
      return viewId;
   }

   @Override
   public String toString() {
      return "CacheJoinInfo{" +
            "consistentHashFactory=" + consistentHashFactory +
            ", hashFunction=" + hashFunction +
            ", numSegments=" + numSegments +
            ", numOwners=" + numOwners +
            ", viewId=" + viewId +
            ", timeout=" + timeout +
            '}';
   }
}
