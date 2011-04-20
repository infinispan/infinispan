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
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import org.infinispan.util.Util;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public class TcpTransportFactory implements TransportFactory {

   private static final Log log = LogFactory.getLog(TcpTransportFactory.class, Log.class);

   /**
    * These are declared volatile as the thread that calls {@link org.infinispan.client.hotrod.impl.transport.TransportFactory#start(java.util.Properties, java.util.Collection, java.util.concurrent.atomic.AtomicInteger)}
    * might(and likely will) be different from the thread(s) that calls {@link #getTransport()} or other methods
    */
   private volatile GenericKeyedObjectPool connectionPool;
   private volatile RequestBalancingStrategy balancer;
   private volatile Collection<InetSocketAddress> servers;
   private volatile ConsistentHash consistentHash;
   private volatile boolean tcpNoDelay;
   private volatile int soTimeout;
   private final ConsistentHashFactory hashFactory = new ConsistentHashFactory();

   @Override
   public void start(ConfigurationProperties cfg, Collection<InetSocketAddress> staticConfiguredServers, AtomicInteger topologyId) {
      hashFactory.init(cfg);
      boolean pingOnStartup = cfg.getPingOnStartup();
      servers = staticConfiguredServers;
      String balancerClass = cfg.getRequestBalancingStrategy();
      balancer = (RequestBalancingStrategy) Util.getInstance(balancerClass);
      tcpNoDelay = cfg.getTcpNoDelay();
      soTimeout = cfg.getSoTimeout();
      PropsKeyedObjectPoolFactory poolFactory = new PropsKeyedObjectPoolFactory(new TransportObjectFactory(this, topologyId, pingOnStartup), cfg.getProperties());
      createAndPreparePool(staticConfiguredServers, poolFactory);
      balancer.setServers(servers);
   }

   /**
    * This will makes sure that, when the evictor thread kicks in the minIdle is set. We don't want to do this is the caller's thread,
    * as this is the user.
    */
   private void createAndPreparePool(Collection<InetSocketAddress> staticConfiguredServers, PropsKeyedObjectPoolFactory poolFactory) {
      connectionPool = (GenericKeyedObjectPool) poolFactory.createPool();
      for (InetSocketAddress addr: staticConfiguredServers) {
         connectionPool.preparePool(addr, false);
      }
   }

   @Override
   public void destroy() {
      connectionPool.clear();
      try {
         connectionPool.close();
      } catch (Exception e) {
         log.errorClosingConnectionPool(e);
      }
   }

   @Override
   public void updateHashFunction(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, short hashFunctionVersion, int hashSpace) {
      ConsistentHash hash = hashFactory.newConsistentHash(hashFunctionVersion);
      if (hash == null) {
         log.noHasHFunctionConfigured(hashFunctionVersion);
      } else {
         hash.init(servers2HashCode, numKeyOwners, hashSpace);
      }
      consistentHash = hash;
   }

   @Override
   public Transport getTransport() {
      InetSocketAddress server = balancer.nextServer();
      return borrowTransportFromPool(server);
   }

   public Transport getTransport(byte[] key) {
      InetSocketAddress server;
      if (consistentHash != null) {
         server = consistentHash.getServer(key);
         if (log.isTraceEnabled()) {
            log.tracef("Using consistent hash for determining the server: %s", server);
         }
      } else {
         server = balancer.nextServer();
         if (log.isTraceEnabled()) {
            log.tracef("Using the balancer for determining the server: %s", server);
         }
      }
      return borrowTransportFromPool(server);
   }

   @Override
   public void releaseTransport(Transport transport) {
      TcpTransport tcpTransport = (TcpTransport) transport;
      if (!tcpTransport.isValid()) {
         try {
            if (log.isTraceEnabled()) {
               log.tracef("Dropping connection as it is no longer valid: %s", tcpTransport);
            }
            connectionPool.invalidateObject(tcpTransport.getServerAddress(), tcpTransport);
         } catch (Exception e) {
            log.couldNoInvalidateConnection(tcpTransport, e);
         }
      } else {
         try {
            connectionPool.returnObject(tcpTransport.getServerAddress(), tcpTransport);
         } catch (Exception e) {
            log.couldNotReleaseConnection(tcpTransport, e);
         } finally {
            logConnectionInfo(tcpTransport.getServerAddress());
         }
      }
   }

   @Override
   public void updateServers(Collection<InetSocketAddress> newServers) {
      synchronized (this) {//only one updateServers at a time. 
         Set<InetSocketAddress> addedServers = new HashSet<InetSocketAddress>(newServers);
         addedServers.removeAll(servers);
         Set<InetSocketAddress> failedServers = new HashSet<InetSocketAddress>(servers);
         failedServers.removeAll(newServers);
         if (log.isTraceEnabled()) {
            log.tracef("Current list: %s", servers);
            log.tracef("New list: ", newServers);
            log.tracef("Added servers: ", addedServers);
            log.tracef("Removed servers: ", failedServers);
         }
         if (failedServers.isEmpty() && newServers.isEmpty()) {
            log.debug("Same list of servers, not changing the pool");
            return;
         }

         //1. first add new servers. For servers that went down, the returned transport will fail for now
         for (InetSocketAddress server : newServers) {
            log.newServerAdded(server);
            try {
               connectionPool.addObject(server);
            } catch (Exception e) {
               log.failedAddingNewServer(server, e);
            }
         }

         //2. now set the server list to the active list of servers. All the active servers (potentially together with some
         // failed servers) are in the pool now. But after this, the pool won't be asked for connections to failed servers,
         // as the balancer will only know about the active servers
         balancer.setServers(newServers);


         //3. Now just remove failed servers
         for (InetSocketAddress server : failedServers) {
            log.removingServer(server);
            connectionPool.clear(server);
         }

         servers.clear();
         servers.addAll(newServers);
      }
   }

   public Collection<InetSocketAddress> getServers() {
      return servers;
   }

   private void logConnectionInfo(InetSocketAddress server) {
      if (log.isTraceEnabled()) {
         log.tracef("For server %s: active = %d; idle = %d", server, connectionPool.getNumActive(server), connectionPool.getNumIdle(server));
      }
   }

   private Transport borrowTransportFromPool(InetSocketAddress server) {
      try {
         return (Transport) connectionPool.borrowObject(server);
      } catch (Exception e) {
         String message = "Could not fetch transport";
         log.couldNotFetchTransport(e);
         throw new TransportException(message, e);
      } finally {
         logConnectionInfo(server);
      }
   }

   public ConsistentHash getConsistentHash() {
      return consistentHash;
   }

   public boolean isTcpNoDelay() {
      return tcpNoDelay;
   }

   @Override
   public int getTransportCount() {
      if (Thread.currentThread().isInterrupted()) { 
         return -1;
      }
      if (connectionPool.getMaxActive() > 0) {
         return connectionPool.getMaxActive() * servers.size();
      } else {
         return 10 * servers.size();
      }
   }

   @Override
   public int getSoTimeout() {
      return soTimeout;
   }

   public RequestBalancingStrategy getBalancer() {
      return balancer;
   }

   public GenericKeyedObjectPool getConnectionPool() {
      return connectionPool;
   }
}
