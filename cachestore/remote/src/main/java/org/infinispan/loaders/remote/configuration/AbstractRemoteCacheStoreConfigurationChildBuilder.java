/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.loaders.remote.configuration;

import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.configuration.cache.AbstractStoreConfigurationChildBuilder;
import org.infinispan.marshall.Marshaller;

/**
 * AbstractRemoteCacheStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class AbstractRemoteCacheStoreConfigurationChildBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements RemoteCacheStoreConfigurationChildBuilder<S> {
   private final RemoteCacheStoreConfigurationBuilder builder;

   protected AbstractRemoteCacheStoreConfigurationChildBuilder(RemoteCacheStoreConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
   }

   @Override
   public RemoteServerConfigurationBuilder addServer() {
      return builder.addServer();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return builder.asyncExecutorFactory();
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder balancingStrategy(String balancingStrategy) {
      return builder.balancingStrategy(balancingStrategy);
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return builder.connectionPool();
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      return builder.connectionTimeout(connectionTimeout);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      return builder.forceReturnValues(forceReturnValues);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      return builder.keySizeEstimate(keySizeEstimate);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder marshaller(String marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      return builder.marshaller(marshaller);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup) {
      return builder.pingOnStartup(pingOnStartup);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      return builder.protocolVersion(protocolVersion);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder rawValues(boolean rawValues) {
      return builder.rawValues(rawValues);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      return builder.remoteCacheName(remoteCacheName);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      return builder.socketTimeout(socketTimeout);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      return builder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder transportFactory(String transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder transportFactory(Class<? extends TransportFactory> transportFactory) {
      return builder.transportFactory(transportFactory);
   }

   @Override
   public RemoteCacheStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      return builder.valueSizeEstimate(valueSizeEstimate);
   }
}
