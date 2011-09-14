/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.interceptors.locking;

import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * Abstractization for logic related to different clustering modes: replicated or distributed.
 * This implements the <a href="http://en.wikipedia.org/wiki/Bridge_pattern">Bridge</a> pattern as described by the GoF:
 * this plays the role of the <b>Implementor</b> and various LockingInterceptors are the <b>Abstraction</b>.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public interface ClusteringDependentLogic {

   boolean localNodeIsOwner(Object key);

   void commitEntry(CacheEntry entry, boolean skipOwnershipCheck);

   public static final class ReplicationLogic implements ClusteringDependentLogic {

      private DataContainer dataContainer;

      @Inject
      public void init(DataContainer dc) {
         this.dataContainer = dc;
      }

      @Override
      public boolean localNodeIsOwner(Object key) {
         return true;
      }

      @Override
      public void commitEntry(CacheEntry entry, boolean skipOwnershipCheck) {
         entry.commit(dataContainer);
      }
   }

   public static final class DistributionLogic implements ClusteringDependentLogic {

      private DistributionManager dm;
      private DataContainer dataContainer;
      private Configuration configuration;

      @Inject
      public void init(DistributionManager dm, DataContainer dataContainer, Configuration configuration) {
         this.dm = dm;
         this.dataContainer = dataContainer;
         this.configuration = configuration;
      }

      @Override
      public boolean localNodeIsOwner(Object key) {
         return dm.getLocality(key).isLocal();
      }

      @Override
      public void commitEntry(CacheEntry entry, boolean skipOwnershipCheck) {
         boolean doCommit = true;
         // ignore locality for removals, even if skipOwnershipCheck is not true
         if (!skipOwnershipCheck && !entry.isRemoved() && !localNodeIsOwner(entry.getKey())) {
            if (configuration.isL1CacheEnabled()) {
               dm.transformForL1(entry);
            } else {
               doCommit = false;
            }
         }
         if (doCommit)
            entry.commit(dataContainer);
         else
            entry.rollback();
      }
   }
}
