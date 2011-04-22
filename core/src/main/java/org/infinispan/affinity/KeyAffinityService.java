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
package org.infinispan.affinity;

import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.remoting.transport.Address;

/**
 * Defines a service that generates keys to be mapped to specific nodes in a distributed(vs. replicated) cluster.
 * The service is instantiated through through one of the factory methods from {@link org.infinispan.affinity.KeyAffinityServiceFactory}.
 * <p/>
 * Sample usage:
 * <p/>
 * <code>
 *    Cache&gt;String,Long&lt; cache = getDistributedCache();
 *    KeyAffinityService&lt;String&gt; service = KeyAffinityServiceFactory.newKeyAffinityService(cache, 100);
 *    ...
 *    String sessionId = sessionObject.getId();
 *    String newCollocatedSession = service.getCollocatedKey(sessionId);
 *
 *    //this will reside on the same node in the cluster
 *    cache.put(newCollocatedSession, someInfo);
 * </code>
 * <p/>
 * Uniqueness: the service does not guarantee that the generated keys are unique. It relies on an
 * {@link org.infinispan.affinity.KeyGenerator} for obtaining and distributing the generated keys. If key uniqueness is
 * needed that should be enforced in the generator.
 * <p/>
 * The service might also drop key generated through the {@link org.infinispan.affinity.KeyGenerator}.
 *
 * @see org.infinispan.affinity.KeyAffinityServiceFactory
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface KeyAffinityService<K> extends Lifecycle {

   /**
    * Returns a key that will be distributed on the cluster node identified by address.
    * @param address identifying the cluster node.
    * @return a key object
    * @throws IllegalStateException if the service has not been started or it is shutdown
    */
   K getKeyForAddress(Address address);

   /**
    * Returns a key that will be distributed on the same node as the supplied key.
    * @param otherKey the key for which we need a collocation
    * @return a key object
    * @throws IllegalStateException if the service has not been started or it is shutdown
    */
   K getCollocatedKey(K otherKey);

   /**
    * Checks weather or not the service is started.
    */
   public boolean isStarted();
}
