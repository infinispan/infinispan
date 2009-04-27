/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.context;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.GlobalTransaction;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A context that contains information pertaining to a given transaction.  These contexts typically have the lifespan of
 * the entire transaction.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see InvocationContext
 * @since 4.0
 */
public interface TransactionContext extends EntryLookup, FlagContainer {
   /**
    * Adds a modification to the modification list.
    *
    * @param command modification
    */
   void addModification(WriteCommand command);

   /**
    * Returns all modifications.  If there are no modifications in this transaction this method will return an empty
    * list.
    *
    * @return list of modifications.
    */
   List<WriteCommand> getModifications();

   /**
    * Adds a modification to the local modification list.
    *
    * @param command command to add to list.  Should not be null.
    * @throws NullPointerException if the command to be added is null.
    */
   void addLocalModification(WriteCommand command);

   /**
    * Returns all modifications that have been invoked with the LOCAL cache mode option.  These will also be in the
    * standard modification list.
    *
    * @return list of LOCAL modifications, or an empty list.
    */
   List<WriteCommand> getLocalModifications();

   /**
    * Adds the key that has been removed in the scope of the current transaction.
    *
    * @param key key that has been removed.
    * @throws NullPointerException if the key is null.
    */
   void addRemovedEntry(Object key);

   /**
    * Gets the list of removed keys.
    *
    * @return list of keys of entries removed in the current transaction scope.  Note that this method will return an
    *         empty list if nothing has been removed.  The list returned is defensively copied.
    */
   List<Object> getRemovedEntries();

   /**
    * Sets the local transaction to be associated with this transaction context.
    *
    * @param tx JTA transaction to associate with.
    */
   void setTransaction(Transaction tx);

   void setGlobalTransaction(GlobalTransaction gtx);

   /**
    * Returns a local transaction associated with this context.
    *
    * @return a JTA transaction
    */
   Transaction getTransaction();

   /**
    * Gets the value of the forceAsyncReplication flag.
    *
    * @return true if the forceAsyncReplication flag is set to true.
    */
   boolean isForceAsyncReplication();

   /**
    * Sets the value of the forceAsyncReplication flag.
    *
    * @param forceAsyncReplication value of forceAsyncReplication
    */
   void setForceAsyncReplication(boolean forceAsyncReplication);

   /**
    * Gets the value of the forceSyncReplication flag.
    *
    * @return true if the forceAsyncReplication flag is set to true.
    */
   boolean isForceSyncReplication();

   /**
    * Sets the value of the forceSyncReplication flag.
    *
    * @param forceSyncReplication value of forceSyncReplication
    */
   void setForceSyncReplication(boolean forceSyncReplication);

   /**
    * Adds a key to the list of uninitialized entry keys created by the cache loader.
    *
    * @param key key to add.  Must not be null.
    */
   void addDummyEntryCreatedByCacheLoader(Object key);

   /**
    * @return a list of uninitialized entries created by the cache loader, or an empty list.
    */
   List<Object> getDummyEntriesCreatedByCacheLoader();

   /**
    * @return true if modifications were registered.
    */
   boolean hasModifications();

   /**
    * @return true if any modifications have been invoked with cache mode being LOCAL.
    */
   boolean hasLocalModifications();

   /**
    * @return true if either there are modifications or local modifications that are not for replicating.
    */
   boolean hasAnyModifications();

   /**
    * Cleans up internal state, freeing up references.
    */
   void reset();

   GlobalTransaction getGobalTransaction();

   /**
    * Retrieves a set of Addresses of caches participating in a given transaction for a specific cache.  Returns null if
    * the participation includes <i>all</i> caches in the cluster (e.g., you are using replication, invalidation or
    * local mode).
    *
    * @return a set of cache addresses
    */
   Set<Address> getTransactionParticipants();

   /**
    * Adds a transaction participant.  This has no effect unless the cache mode used is DIST.
    *
    * @param addresses address to add
    */
   void addTransactionParticipants(Collection<Address> addresses);
}
