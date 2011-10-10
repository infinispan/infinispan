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

package org.infinispan.tx.recovery;

import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.RecoveryHandlerTest")
public class RecoveryHandlerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.fluent().transaction().recovery().useSynchronization(false);
      createCluster(config, 2);
      waitForClusterToForm();
   }

   public void testRecoveryHandler() throws Exception {
      final XAResource xaResource = cache(0).getAdvancedCache().getXAResource();
      final Xid[] recover = xaResource.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
      assert recover != null && recover.length == 0;
   }
}
