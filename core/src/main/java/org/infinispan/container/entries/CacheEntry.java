/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.container.entries;

import org.infinispan.container.DataContainer;

import java.util.Map;

/**
 * An entry that is stored in the data container
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface CacheEntry extends Map.Entry<Object, Object> {

   /**
    * Tests whether the entry represents a null value, typically used for repeatable read.
    *
    * @return true if this represents a null, false otherwise.
    */
   boolean isNull();

   /**
    * @return true if this entry has changed since being read from the container, false otherwise.
    */
   boolean isChanged();

   /**
    * @return true if this entry has been newly created, false otherwise.
    */
   boolean isCreated();

   /**
    * @return true if this entry has been removed since being read from the container, false otherwise.
    */
   boolean isRemoved();


   /**
    * @return true if this entry has been evicted since being read from the container, false otherwise.
    */
   boolean isEvicted();

   /**
    * @return true if this entry is still valid, false otherwise.
    */
   boolean isValid();

   /**
    * Retrieves the key to this entry
    *
    * @return a key
    */
   Object getKey();

   /**
    * Retrieves the value of this entry
    *
    * @return the value of the entry
    */
   Object getValue();

   /**
    * @return retrieves the lifespan of this entry.  -1 means an unlimited lifespan.
    */
   long getLifespan();

   /**
    * @return the maximum allowed time for which this entry can be idle, after which it is considered expired.
    */
   long getMaxIdle();

   /**
    * Sets the maximum idle time of the entry.
    *
    * @param maxIdle maxIdle to set
    */
   void setMaxIdle(long maxIdle);

   /**
    * Sets the lifespan of the entry.
    *
    * @param lifespan lifespan to set
    */
   void setLifespan(long lifespan);

   /**
    * Sets the value of the entry, returning the previous value
    *
    * @param value value to set
    * @return previous value
    */
   Object setValue(Object value);

   /**
    * Commits changes
    *
    * @param container data container to commit to
    */
   void commit(DataContainer container);

   /**
    * Rolls back changes
    */
   void rollback();

   void setCreated(boolean created);

   void setRemoved(boolean removed);

   void setEvicted(boolean evicted);

   void setValid(boolean valid);

   /**
    * @return true if this entry is a placeholder for the sake of acquiring a lock; and false if it is a real entry. 
    */
   boolean isLockPlaceholder();

   /**
    * If the entry is marked as removed and doUndelete==true then the removed and valid flags are set to true.
    */
   boolean undelete(boolean doUndelete);
}
