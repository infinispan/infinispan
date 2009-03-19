/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.horizon.container;

import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.loader.StoredEntry;

import java.util.Set;

/**
 * The main internal data structure which stores entries
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DataContainer {
   Object get(Object k);

   void put(Object k, Object v, long lifespan);

   boolean containsKey(Object k);

   Object remove(Object k);

   int size();

   void clear();

   Set<Object> keySet();

   long getModifiedTimestamp(Object key);

   /**
    * Purges entries that have passed their expiry time, returning a set of keys that have been purged.
    *
    * @return set of keys that have been purged.
    */
   Set<Object> purgeExpiredEntries();

   StoredEntry createEntryForStorage(Object key);

   CachedValue getEntry(Object k);

   Set<StoredEntry> getAllEntriesForStorage();
}
