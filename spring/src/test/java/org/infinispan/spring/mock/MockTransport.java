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

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class MockTransport implements Transport {

   @Override
   public Map<Address, Response> invokeRemotely(final Collection<Address> recipients,
            final ReplicableCommand rpcCommand, final ResponseMode mode, final long timeout,
            final boolean usePriorityQueue, final ResponseFilter responseFilter) throws Exception {
      return null;
   }

   @Override
   public boolean isCoordinator() {
      return false;
   }

   @Override
   public Address getCoordinator() {
      return null;
   }

   @Override
   public Address getAddress() {
      return null;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      return null;
   }

   @Override
   public List<Address> getMembers() {
      return null;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

   @Override
   public int getViewId() {
      return 0;
   }

   @Override
   public Log getLog() {
      return null;
   }

   @Override
   public boolean isMulticastCapable() {
      return false;
   }
}
