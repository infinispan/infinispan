/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.jmx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "jmx.CacheClearTest")
public class CacheClearTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = CacheClearTest.class.getSimpleName();
   private MBeanServer server;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.globalJmxStatistics().jmxDomain(JMX_DOMAIN).mBeanServerLookup(new PerThreadMBeanServerLookup()).enable();
      ConfigurationBuilder dcc = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      dcc.transaction().autoCommit(false);
      dcc.jmxStatistics().enable();
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return new DefaultCacheManager(gcb.build(), dcc.build());
   }

   public void testClear() throws Exception {
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN);
      tm().begin();
      cache().put("k","v");
      tm().commit();
      assertFalse(cache().isEmpty());
      server.invoke(defaultOn, "clear", new Object[]{}, new String[]{});
      assertTrue(cache().isEmpty());
   }
}
