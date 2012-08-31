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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;

/**
 * This class holds statistics about a consistent hash.
 *
 * @author Dan Berindei
 * @since 5.2
 */
class CHStatistics {
   private final Map<Address, Integer> nodes;
   private final int[] primaryOwned;
   private final int[] owned;

   public CHStatistics(List<Address> nodes) {
      this.nodes = new HashMap<Address, Integer>(nodes.size());
      for (int i = 0; i < nodes.size(); i++) {
         this.nodes.put(nodes.get(i), i);
      }
      this.primaryOwned = new int[nodes.size()];
      this.owned = new int[nodes.size()];
   }

   public int getPrimaryOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         return 0;
      return primaryOwned[i];
   }

   public int getOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         return 0;
      return owned[i];
   }

   public void incPrimaryOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      primaryOwned[i]++;
   }

   public void incOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      owned[i]++;
   }

   public void decPrimaryOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      primaryOwned[i]--;
   }

   public void decOwned(Address a) {
      Integer i = nodes.get(a);
      if (i == null)
         throw new IllegalArgumentException("Trying to modify statistics for a node that doesn't exist: " + a);

      owned[i]--;
   }
}
