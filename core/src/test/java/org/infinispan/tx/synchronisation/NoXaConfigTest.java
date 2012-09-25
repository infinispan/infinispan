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

package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronization.NoXaConfigTest")
public class NoXaConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml("configs/no-xa-config.xml");
   }

   public void testConfig() {
      assert cacheManager.getCache("syncEnabled").getConfiguration().isUseSynchronizationForTransactions();
      assert cacheManager.getCache("notSpecified").getConfiguration().isUseSynchronizationForTransactions();

      cacheManager.getCache("syncAndRecovery");
   }

   public void testConfigOverride() {
      Configuration configuration = getDefaultStandaloneConfig(true);
      configuration.fluent().transaction().useSynchronization(true);
      cacheManager.defineConfiguration("newCache", configuration);
      assert cacheManager.getCache("newCache").getConfiguration().isUseSynchronizationForTransactions();
   }
}
