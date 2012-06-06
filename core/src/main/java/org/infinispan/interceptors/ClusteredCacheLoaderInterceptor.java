/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.interceptors;

import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Transport;

import java.util.Set;

/**
 * The same as a regular cache loader interceptor, except that it contains additional logic to force loading from the
 * cache loader if needed on a remote node, in certain conditions.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ClusteredCacheLoaderInterceptor extends CacheLoaderInterceptor {

   private Configuration.CacheMode cacheMode;
   private boolean isWriteSkewConfigured;
   private Transport transport;
   private DistributionManager distributionManager;

   @Inject
   private void injectClusteredCacheLoaderInterceptorDependencies(Transport transport, DistributionManager distributionManager) {
      this.transport = transport;
      this.distributionManager = distributionManager;
   }
   
   
   @Start(priority = 15)
   private void startClusteredCacheLoaderInterceptor() {
      cacheMode = configuration.getCacheMode();
      // For now the coordinator/primary data owner may need to load from the cache store, even if
      // this is a remote call, if write skew checking is enabled.  Once ISPN-317 is in, this may also need to
      // happen if running in distributed mode and eviction is enabled.
      isWriteSkewConfigured = configuration.isWriteSkewCheck() && cacheMode.isClustered();
   }

   @Override
   protected boolean forceLoad(Object key, Set<Flag> flags) {
      return isWriteSkewConfigured && ((cacheMode.isReplicated() && transport.isCoordinator()) ||
            (cacheMode.isDistributed() && distributionManager.getPrimaryLocation(key).equals(transport.getAddress())));
   }
}
