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

public interface RemoteCacheStoreConfigurationChildBuilder {

   RemoteServerConfigurationBuilder addServer();

   ExecutorFactoryConfigurationBuilder asyncExecutorFactory();

   RemoteCacheStoreConfigurationBuilder balancingStrategy(String balancingStrategy);

   ConnectionPoolConfigurationBuilder connectionPool();

   RemoteCacheStoreConfigurationBuilder connectionTimeout(long connectionTimeout);

   RemoteCacheStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues);

   RemoteCacheStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate);

   RemoteCacheStoreConfigurationBuilder marshaller(String marshaller);

   RemoteCacheStoreConfigurationBuilder pingOnStartup(boolean pingOnStartup);

   RemoteCacheStoreConfigurationBuilder protocolVersion(String protocolVersion);

   RemoteCacheStoreConfigurationBuilder remoteCacheName(String remoteCacheName);

   RemoteCacheStoreConfigurationBuilder socketTimeout(long socketTimeout);

   RemoteCacheStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   RemoteCacheStoreConfigurationBuilder transportFactory(String transportFactory);

   RemoteCacheStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate);

}