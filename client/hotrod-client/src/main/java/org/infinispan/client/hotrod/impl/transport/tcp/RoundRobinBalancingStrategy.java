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
package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Round-robin implementation for {@link org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class RoundRobinBalancingStrategy implements RequestBalancingStrategy {

   private static final Log log = LogFactory.getLog(RoundRobinBalancingStrategy.class);

   private int index = 0;

   private InetSocketAddress[] servers;

   @Override
   public void setServers(Collection<InetSocketAddress> servers) {
      this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
      // keep the old index if possible so that we don't produce more requests for the first server
      if (index >= this.servers.length) {
         index = 0;
      }
      if (log.isTraceEnabled()) {
         log.trace("New server list is: " + Arrays.toString(this.servers));
      }
   }

   /**
    * Multiple threads might call this method at the same time.
    */
   @Override
   public InetSocketAddress nextServer() {
      InetSocketAddress server = getServerByIndex(index++);
      // don't allow index to overflow and have a negative value
      if (index >= servers.length)
         index = 0;
      return server;
   }

   /**
    * Returns same value as {@link #nextServer()} without modifying indexes/state.
    */
   public InetSocketAddress dryRunNextServer() {
      return getServerByIndex(index);
   }

   private InetSocketAddress getServerByIndex(int pos) {
      InetSocketAddress server = servers[pos];
      if (log.isTraceEnabled()) {
         log.trace("Returning server: " + server);
      }
      return server;
   }

   public InetSocketAddress[] getServers() {
      return servers;
   }

   public int getNextPosition() {
      return  index;
   }
}
