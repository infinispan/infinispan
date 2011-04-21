/*
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
package org.infinispan.config;

import org.infinispan.config.Configuration.EvictionType;
import org.infinispan.config.GlobalConfiguration.TransportType;
import org.infinispan.loaders.decorators.SingletonStoreConfig;

/**
 * ConfigurationValidatingVisitor checks semantic validity of InfinispanConfiguration instance.
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class ConfigurationValidatingVisitor extends AbstractConfigurationBeanVisitor {
   private TransportType tt = null;

   @Override
   public void visitSingletonStoreConfig(SingletonStoreConfig ssc) {
      if (tt == null && ssc.isSingletonStoreEnabled()) throw new ConfigurationException("Singleton store configured without transport being configured");
   }

   @Override
   public void visitTransportType(TransportType tt) {
      this.tt = tt;
   }

   @Override
   public void visitConfiguration(Configuration bean) {
   }

   @Override
   public void visitClusteringType(Configuration.ClusteringType clusteringType) {
      if (clusteringType.mode.isDistributed() && clusteringType.async.useReplQueue)
         throw new ConfigurationException("Use of the replication queue is invalid when using DISTRIBUTED mode.");

      if (clusteringType.mode.isSynchronous() && clusteringType.async.useReplQueue)
         throw new ConfigurationException("Use of the replication queue is only allowed with an ASYNCHRONOUS cluster mode.");
   }
   
   public void visitEvictionType(EvictionType et) {
      if (et.strategy.isEnabled() && et.maxEntries <= 0)
         throw new ConfigurationException("Eviction maxEntries value cannot be less than or equal to zero if eviction is enabled");
   }
}
