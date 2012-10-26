/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.distribution;

import org.infinispan.remoting.transport.TopologyAwareAddress;

/**
 * Mock TopologyAwareAddress to be used in tests.
 * We only care about the addressNum for equality, so we don't override compareTo(), equals() and hashCode().
 *
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 5.0
 */
public class TestTopologyAwareAddress extends TestAddress implements TopologyAwareAddress {
   String siteId, rackId, machineId;

   public TestTopologyAwareAddress(int addressNum, String siteId, String rackId, String machineId) {
      super(addressNum);
      this.siteId = siteId;
      this.rackId = rackId;
      this.machineId = machineId;
   }

   public TestTopologyAwareAddress(int addressNum) {
      this(addressNum, null, null, null);
   }

   @Override
   public String toString() {
      return super.toString() + "|" + machineId + "|" + rackId + "|" + siteId;
   }

   @Override
   public boolean isSameSite(TopologyAwareAddress addr) {
      return siteId != null ? siteId.equals(addr.getSiteId()) : addr.getSiteId() == null;
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      if (!isSameSite(addr))
         return false;
      return rackId != null ? rackId.equals(addr.getRackId()) : addr.getRackId() == null;
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      if (!isSameSite(addr) || !isSameRack(addr))
         return false;
      return machineId != null ? machineId.equals(addr.getMachineId()) : addr.getMachineId() == null;
   }

   public String getSiteId() {
      return siteId;
   }

   public void setSiteId(String siteId) {
      this.siteId = siteId;
   }

   public String getRackId() {

      return rackId;
   }

   public void setRackId(String rackId) {
      this.rackId = rackId;
   }

   public String getMachineId() {
      return machineId;
   }

   public void setMachineId(String machineId) {
      this.machineId = machineId;
   }
}
