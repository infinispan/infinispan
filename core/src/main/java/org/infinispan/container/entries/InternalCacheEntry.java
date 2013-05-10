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

/**
 * Interface for internal cache entries that expose whether an entry has expired.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 4.0
 */
public interface InternalCacheEntry extends CacheEntry, Cloneable {

   /**
    * @param the current time as defined by {@link System#currentTimeMillis()}
    * @return true if the entry has expired; false otherwise
    * @since 5.1
    */
   boolean isExpired(long now);

   /**
    * @return true if the entry has expired; false otherwise
    */
   boolean isExpired();

   /**
    * @return true if the entry can expire, false otherwise
    */
   boolean canExpire();

   /**
    * @return timestamp when the entry was created
    */
   long getCreated();

   /**
    * @return timestamp when the entry was last used
    */
   long getLastUsed();

   /**
    * Only used with entries that have a lifespan, this determines when an entry is due to expire.
    *
    * @return timestamp when the entry is due to expire, or -1 if it doesn't have a lifespan
    */
   long getExpiryTime();

   /**
    * Updates access timestamps on this instance
    */
   void touch();

   /**
    * Updates access timestamps on this instance to a specified time
    * @param currentTimeMillis
    */
   void touch(long currentTimeMillis);

   /**
    * "Reincarnates" an entry.  Essentially, resets the 'created' timestamp of the entry to the current time.
    */
   void reincarnate();

   /**
    * Creates a representation of this entry as an {@link org.infinispan.container.entries.InternalCacheValue}. The main
    * purpose of this is to provide a representation that does <i>not</i> have a reference to the key. This is useful in
    * situations where the key is already known or stored elsewhere, making serialization and deserialization more
    * efficient.
    * <p/>
    * Note that this should not be used to optimize memory overhead, since the saving of an additional reference to a
    * key (a single object reference) does not warrant the cost of constructing an InternalCacheValue.  This <i>only</i>
    * makes sense when marshalling is involved, since the cost of marshalling the key again can be sidestepped using an
    * InternalCacheValue if the key is already known/marshalled.
    * <p/>
    *
    * @return a new InternalCacheValue encapsulating this InternalCacheEntry's value and expiration information.
    */
   InternalCacheValue toInternalCacheValue();

   InternalCacheEntry clone();
}
