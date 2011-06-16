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
package org.infinispan.interceptors;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * A subclass of the locking interceptor that is able to differentiate committing changes on a ReadCommittedEntry for
 * storage in the main cache or in L1, used by DIST
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistLockingInterceptor extends LockingInterceptor {
   DistributionManager dm;

   @Inject
   public void injectDistributionManager(DistributionManager dm) {
      this.dm = dm;
   }

   protected void commitEntry(CacheEntry entry, boolean skipOwnershipCheck) {
      boolean doCommit = true;
      // ignore locality for removals, even if skipOwnershipCheck is not true
      if (!skipOwnershipCheck && !entry.isRemoved() && !dm.getLocality(entry.getKey()).isLocal()) {
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
