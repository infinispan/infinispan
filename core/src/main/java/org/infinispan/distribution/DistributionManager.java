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
package org.infinispan.distribution;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A component that manages the distribution of elements across a cache cluster
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DistributionManager {

   /**
    * Checks whether a key is mapped to the local node.
    * <p />
    * <b>Do not use!</b> This API is buggy in that it doesn't take into account changing ownerships and can introduce
    * race conditions if results are relied upon.  Please use {@link #getLocality(Object)} instead.
    * @param key key to test
    * @return true if local, false otherwise.
    * @deprecated
    */
   @Deprecated
   boolean isLocal(Object key);

   /**
    * Returns the data locality characteristics of a given key.
    * @param key key to test
    * @return a DataLocality that allows you to test whether a key is mapped to the local node or not, and the degree of
    * certainty of such a result.
    */
   DataLocality getLocality(Object key);

   /**
    * Locates a key in a cluster.  The returned addresses <i>may not</i> be owners of the keys if a rehash happens to be
    * in progress or is pending, so when querying these servers, invalid responses should be checked for and the next
    * address checked accordingly.
    *
    * @param key key to test
    * @return a list of addresses where the key may reside
    */
   List<Address> locate(Object key);

   /**
    * Returns the first Address containing the key.  Equivalent to returning the first element of {@link #locate(Object)}
    * @param key key to test
    * @return the first address on which the key may reside
    */
   Address getPrimaryLocation(Object key);

   /**
    * Locates a list of keys in a cluster.  Like {@link #locate(Object)} the returned addresses <i>may not</i> be owners
    * of the keys if a rehash happens to be in progress or is pending, so when querying these servers, invalid responses
    * should be checked for and the next address checked accordingly.
    *
    * @param keys list of keys to test
    * @return a list of addresses where the key may reside
    */
   Map<Object, List<Address>> locateAll(Collection<Object> keys);

   /**
    * Same as {@link #locateAll(java.util.Collection)}, but the list of addresses only contains numOwners owners.
    */
   Map<Object, List<Address>> locateAll(Collection<Object> keys, int numOwners);

   /**
    * Transforms a cache entry so it is marked for L1 rather than the primary cache data structure.  This should be done
    * if it is deemed that the entry is targeted for L1 storage rather than storage in the primary data container.
    *
    * @param entry entry to transform
    */
   void transformForL1(CacheEntry entry);

   /**
    * Retrieves a cache entry from a remote source.  Would typically involve an RPC call using a {@link org.infinispan.commands.remote.ClusteredGetCommand}
    * and some form of quorum of responses if the responses returned are inconsistent - often the case if there is a
    * rehash in progress, involving nodes that the key maps to.
    *
    * @param key key to look up
    * @return an internal cache entry, or null if it cannot be located
    */
   InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx, boolean acquireRemoteLock) throws Exception;

   /**
    * Retrieves the consistent hash instance currently in use, an instance of the configured ConsistentHash
    * class (which defaults to {@link org.infinispan.distribution.ch.DefaultConsistentHash}.
    *
    * @return a ConsistentHash instance
    */
   ConsistentHash getConsistentHash();

   /**
    * Sets the consistent hash implementation in use.
    * @param consistentHash consistent hash to set to
    * @return previous consistent hash, the last one for which rehash completed
    */
   ConsistentHash setConsistentHash(ConsistentHash consistentHash);

   /**
    * Tests whether a given key is affected by a rehash that may be in progress.  If no rehash is in progress, this method
    * returns false.  Helps determine whether additional steps are necessary in handling an operation with a given key.
    *
    * @param key key to test
    * @return whether a key is affected by a rehash
    */
   boolean isAffectedByRehash(Object key);

   /**
    * Tests whether a rehash is in progress
    * @return true if a rehash is in progress, false otherwise
    */
   boolean isRehashInProgress();

   /**
    * Tests whether the current instance has completed joining the cluster
    * @return true if join is in progress, false otherwise
    */
   boolean isJoinComplete();

   /**
    * A helper method that retrieves a list of nodes affected by operations on a set of keys.  This helper will in turn
    * call {@link #locateAll(java.util.Collection)} and then combine the result addresses.
    *
    * @param affectedKeys keys to locate
    * @return a list of addresses which represent a combined set of all addresses affected by the set of keys.
    */
   Collection<Address> getAffectedNodes(Collection<Object> affectedKeys);
}

