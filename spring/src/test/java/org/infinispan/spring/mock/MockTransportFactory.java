/**
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
 *   ~
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

package org.infinispan.spring.mock;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

public final class MockTransportFactory implements TransportFactory {

   @Override
   public Transport getTransport() {
      return null;
   }

   @Override
   public void releaseTransport(final Transport transport) {
   }

   @Override
   public void start(Codec codec, Configuration configuration, AtomicInteger topologyId) {
   }

   @Override
   public void updateServers(final Collection<SocketAddress> newServers) {
   }

   @Override
   public void destroy() {
   }

   @Override
   public void updateHashFunction(final Map<SocketAddress, Set<Integer>> servers2Hash,
            final int numKeyOwners, final short hashFunctionVersion, final int hashSpace) {
   }

   @Override
   public Transport getTransport(final byte[] key) {
      return null;
   }

   @Override
   public boolean isTcpNoDelay() {
      return false;
   }

   @Override
   public int getTransportCount() {
      return 0;
   }

   @Override
   public int getSoTimeout() {
      return 1000;
   }

   @Override
   public int getConnectTimeout() {
      return 1000;
   }

   @Override
   public void invalidateTransport(SocketAddress serverAddress, Transport transport) {
      // Do nothing

   }

   @Override
   public ConsistentHashFactory getConsistentHashFactory() {
      return null;
   }
}
