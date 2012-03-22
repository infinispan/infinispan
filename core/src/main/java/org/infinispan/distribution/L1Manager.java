/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Manages the L1 Cache, in particular recording anyone who is going to cache an
 * a command that a node responds to so that a unicast invalidation can be sent
 * later if needed.
 * 
 * @author Pete Muir
 * 
 */
@Scope(Scopes.NAMED_CACHE)
public interface L1Manager {

	/**
	 * Records a request that will be cached in another nodes L1
	 */
	void addRequestor(Object key, Address requestor);

	/**
	 * Flushes a cache (using unicast or multicast) for a set of keys
	 */
	NotifyingNotifiableFuture<Object> flushCache(Collection<Object> keys, Object retval, Address origin,
                                                boolean assumeOriginKeptEntryInL1);

   Future<Object> flushCacheWithSimpleFuture(Collection<Object> keys, Object retval, Address origin,
                                             boolean assumeOriginKeptEntryInL1);
}
