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

/**
 * This class contains the information that a cache needs to supply to the coordinator when starting up.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheJoinInfo {
   private String consistentHashFactoryClass;
   private int numSegments;
   private int numOwners;
   private int timeout;

   public String getConsistentHashFactoryClass() {
      return consistentHashFactoryClass;
   }

   public void setConsistentHashFactoryClass(String consistentHashFactoryClass) {
      this.consistentHashFactoryClass = consistentHashFactoryClass;
   }

   public int getNumSegments() {
      return numSegments;
   }

   public void setNumSegments(int numSegments) {
      this.numSegments = numSegments;
   }

   public int getNumOwners() {
      return numOwners;
   }

   public void setNumOwners(int numOwners) {
      this.numOwners = numOwners;
   }

   public int getTimeout() {
      return timeout;
   }

   public void setTimeout(int timeout) {
      this.timeout = timeout;
   }
}
