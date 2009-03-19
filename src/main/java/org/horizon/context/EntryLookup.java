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
package org.horizon.context;

import org.horizon.container.MVCCEntry;
import org.horizon.util.BidirectionalMap;

import java.util.Map;

/**
 * Interface that can look up MVCC wrapped entries.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public interface EntryLookup {
   /**
    * Retrieves an entry from the collection of looked up entries in the current scope.
    * <p/>
    * If a transaction is in progress, implementations should delegate to the same method in {@link
    * TransactionContext}.
    * <p/>
    *
    * @param key key to look up
    * @return an entry, or null if it cannot be found.
    */
   MVCCEntry lookupEntry(Object key);

   /**
    * Retrieves a map of entries looked up within the current scope.
    * <p/>
    * If a transaction is in progress, implementations should delegate to the same method in {@link
    * TransactionContext}.
    * <p/>
    *
    * @return a map of looked up entries.
    */
   BidirectionalMap<Object, MVCCEntry> getLookedUpEntries();

   /**
    * Puts an entry in the registry of looked up entries in the current scope.
    * <p/>
    * If a transaction is in progress, implementations should delegate to the same method in {@link
    * TransactionContext}.
    * <p/>
    *
    * @param key key to store
    * @param e   entry to store
    */
   void putLookedUpEntry(Object key, MVCCEntry e);

   void putLookedUpEntries(Map<Object, MVCCEntry> lookedUpEntries);

   void removeLookedUpEntry(Object key);

   /**
    * Clears the collection of entries looked up
    */
   void clearLookedUpEntries();

   /**
    * Note that if a transaction is in scope, implementations should test this lock from on {@link TransactionContext}.
    * Using this method should always ensure locks checked in the appropriate scope.
    *
    * @param key lock to test
    * @return true if the lock being tested is already held in the current scope, false otherwise.
    */
   boolean hasLockedKey(Object key);

   /**
    * @return true if the context contains modifications, false otherwise
    */
   boolean isContainsModifications();

   /**
    * Sets whether modifications have been made in the current context
    */
   void setContainsModifications(boolean b);

   /**
    * @return true if the context contains locks, false otherwise
    */
   boolean isContainsLocks();

   /**
    * Sets whether locks have been acquired in the current context
    */
   void setContainsLocks(boolean b);
}
