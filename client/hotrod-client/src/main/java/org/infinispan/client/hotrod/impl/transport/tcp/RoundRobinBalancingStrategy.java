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

   private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
   private final Lock readLock = readWriteLock.readLock();
   private final Lock writeLock = readWriteLock.writeLock();
   private final AtomicInteger index = new AtomicInteger(0);

   private volatile InetSocketAddress[] servers;

   @Override
   public void setServers(Collection<InetSocketAddress> servers) {
      writeLock.lock();
      try {
         this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
         index.set(0);
         if (log.isTraceEnabled()) {
            log.trace("New server list is: " + Arrays.toString(this.servers) + ". Resetting index to 0");
         }
      } finally {
         writeLock.unlock();
      }
   }

   /**
    * Multiple threads might call this method at the same time.
    */
   @Override
   public InetSocketAddress nextServer() {
      readLock.lock();
      try {
         InetSocketAddress server = getServerByIndex(index.getAndIncrement());
         return server;
      } finally {
         readLock.unlock();
      }
   }

   /**
    * Returns same value as {@link #nextServer()} without modifying indexes/state.
    */
   public InetSocketAddress dryRunNextServer() {
      return getServerByIndex(index.get());
   }

   private InetSocketAddress getServerByIndex(int val) {
      int pos = val % servers.length;
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
      return  index.get() % servers.length;
   }
}
