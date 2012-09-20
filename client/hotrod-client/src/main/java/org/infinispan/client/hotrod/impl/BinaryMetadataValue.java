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
package org.infinispan.client.hotrod.impl;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
public class BinaryMetadataValue extends BinaryVersionedValue {

   private final long created;
   private final int lifespan;
   private final long lastUsed;
   private final int maxIdle;

   public BinaryMetadataValue(long created, int lifespan, long lastUsed, int maxIdle, long version, byte[] value) {
      super(version, value);
      this.created = created;
      this.lifespan = lifespan;
      this.lastUsed = lastUsed;
      this.maxIdle = maxIdle;
   }

   public long getCreated() {
      return created;
   }

   public int getLifespan() {
      return lifespan;
   }

   public long getLastUsed() {
      return lastUsed;
   }

   public int getMaxIdle() {
      return maxIdle;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (int) (created ^ (created >>> 32));
      result = prime * result + (int) (lastUsed ^ (lastUsed >>> 32));
      result = prime * result + lifespan;
      result = prime * result + maxIdle;
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (getClass() != obj.getClass())
         return false;
      BinaryMetadataValue other = (BinaryMetadataValue) obj;
      if (created != other.created)
         return false;
      if (lastUsed != other.lastUsed)
         return false;
      if (lifespan != other.lifespan)
         return false;
      if (maxIdle != other.maxIdle)
         return false;
      return true;
   }
}
