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
import org.infinispan.container.entries.versioned.Versioned;
import org.infinispan.container.versioning.EntryVersion;

import java.util.Map;

/**
 * An entry that is stored in the data container
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface CacheEntry extends Map.Entry<Object, Object>, Versioned {

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
    * @return true if the entry was loaded from a cache store.
    */
   boolean isLoaded();

   /**
    * Retrieves the key to this entry
    *
    * @return a key
    */
   @Override
   Object getKey();

   /**
    * Retrieves the value of this entry
    *
    * @return the value of the entry
    */
   @Override
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
   @Override
   Object setValue(Object value);

   /**
    * Commits changes
    *
    * @param container data container to commit to
    */
   void commit(DataContainer container, EntryVersion newVersion);

   /**
    * Rolls back changes
    */
   void rollback();

   void setChanged(boolean changed);

   void setCreated(boolean created);

   void setRemoved(boolean removed);

   void setEvicted(boolean evicted);

   void setValid(boolean valid);

   void setLoaded(boolean loaded);

   /**
    * If the entry is marked as removed and doUndelete==true then the "valid" flag is set to true and "removed"
    * flag is set to false.
    */
   boolean undelete(boolean doUndelete);
}
