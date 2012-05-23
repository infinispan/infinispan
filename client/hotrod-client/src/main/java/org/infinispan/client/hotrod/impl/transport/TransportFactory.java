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
package org.infinispan.client.hotrod.impl.transport;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;

/**
 * Transport factory for building and managing {@link org.infinispan.client.hotrod.impl.transport.Transport} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TransportFactory {

   Transport getTransport();

   void releaseTransport(Transport transport);

   void start(Codec codec, Configuration configuration, AtomicInteger topologyId);

   void updateServers(Collection<SocketAddress> newServers);

   void destroy();

   void updateHashFunction(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, short hashFunctionVersion, int hashSpace);

   ConsistentHashFactory getConsistentHashFactory();

   Transport getTransport(byte[] key);

   boolean isTcpNoDelay();

   int getTransportCount();

   int getSoTimeout();

   int getConnectTimeout();

   void invalidateTransport(SocketAddress serverAddress, Transport transport);

   SSLContext getSSLContext();
}
