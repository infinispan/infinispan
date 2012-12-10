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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.CacheException;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
* Created with
*
* @author Dan Berindei
* @since 5.2
*/
class RebalanceConfirmationCollector {
   private final static Log log = LogFactory.getLog(RebalanceConfirmationCollector.class);
   
   private final String cacheName;
   private final int topologyId;
   private final Set<Address> confirmationsNeeded;

   public RebalanceConfirmationCollector(String cacheName, int topologyId, Collection<Address> members) {
      this.cacheName = cacheName;
      this.topologyId = topologyId;
      this.confirmationsNeeded = new HashSet<Address>(members);
      log.tracef("Initialized rebalance confirmation collector %d@%s, initial list is %s",
            topologyId, cacheName, confirmationsNeeded);
   }

   /**
    * @return {@code true} if everyone has confirmed
    */
   public boolean confirmRebalance(Address node, int receivedTopologyId) {
      synchronized (this) {
         if (topologyId != receivedTopologyId) {
            throw new CacheException(String.format("Received invalid rebalance confirmation from %s " +
                  "for cache %s, expecting topology id %d but got %d", node, cacheName, topologyId, receivedTopologyId));
         }

         boolean removed = confirmationsNeeded.remove(node);
         if (!removed) {
            log.tracef("Rebalance confirmation collector %d@%s ignored confirmation for %s, which is already confirmed",
                  topologyId, cacheName, node);
            return false;
         }

         log.tracef("Rebalance confirmation collector %d@%s received confirmation for %s, remaining list is %s",
               topologyId, cacheName, node, confirmationsNeeded);
         return confirmationsNeeded.isEmpty();
      }
   }

   /**
    * @return {@code true} if everyone has confirmed
    */
   public boolean updateMembers(Collection<Address> newMembers) {
      synchronized (this) {
         // only return true the first time
         boolean modified = confirmationsNeeded.retainAll(newMembers);
         log.tracef("Rebalance confirmation collector %d@%s members list updated, remaining list is %s",
               topologyId, cacheName, confirmationsNeeded);
         return modified && confirmationsNeeded.isEmpty();
      }
   }

   @Override
   public String toString() {
      return "RebalanceConfirmationCollector{" +
            "cacheName=" + cacheName +
            "topologyId=" + topologyId +
            ", confirmationsNeeded=" + confirmationsNeeded +
            '}';
   }
}
