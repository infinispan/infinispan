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
package org.infinispan.jmx;

import java.lang.reflect.Method;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.CacheException;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import static org.infinispan.test.TestingUtil.*;

import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.CacheMBeanTest")
public class CacheMBeanTest extends SingleCacheManagerTest {
   private static final Log log = LogFactory.getLog(CacheMBeanTest.class);
   public static final String JMX_DOMAIN = CacheMBeanTest.class.getSimpleName();
   private MBeanServer server;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(JMX_DOMAIN);
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return cacheManager;
   }
   
   public void testStartStopManagedOperations() throws Exception {
      ObjectName defaultOn = getCacheObjectName(JMX_DOMAIN);
      ObjectName managerON = getCacheManagerObjectName(JMX_DOMAIN);
      server.invoke(managerON, "startCache", new Object[]{}, new String[]{});
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      server.invoke(defaultOn, "stop", new Object[]{}, new String[]{});
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      server.invoke(defaultOn, "start", new Object[]{}, new String[]{});
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      server.invoke(defaultOn, "stop", new Object[]{}, new String[]{});
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      server.invoke(defaultOn, "start", new Object[]{}, new String[]{});
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("1");
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      server.invoke(defaultOn, "stop", new Object[]{}, new String[]{});
      assert server.getAttribute(managerON, "CreatedCacheCount").equals("1");
      assert server.getAttribute(managerON, "RunningCacheCount").equals("0");
      assert ComponentStatus.TERMINATED.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
   }
   
   public void testManagerStopRemovesCacheMBean(Method m) throws Exception {
      final String otherJmxDomain = getMethodSpecificJmxDomain(m, JMX_DOMAIN);
      ObjectName defaultOn = getCacheObjectName(otherJmxDomain);
      ObjectName galderOn = getCacheObjectName(otherJmxDomain, "galder(local)");
      ObjectName managerON = getCacheManagerObjectName(otherJmxDomain);
      CacheContainer otherContainer = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(otherJmxDomain);
      server.invoke(managerON, "startCache", new Object[]{}, new String[]{});
      server.invoke(managerON, "startCache", new Object[]{"galder"}, new String[]{String.class.getName()});
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(defaultOn, "CacheStatus"));
      assert ComponentStatus.RUNNING.toString().equals(server.getAttribute(galderOn, "CacheStatus"));
      otherContainer.stop();
      try {
         log.info(server.getMBeanInfo(managerON));
         assert false : "Failure expected, " + managerON + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
      try {
         log.info(server.getMBeanInfo(defaultOn));
         assert false : "Failure expected, " + defaultOn + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
      try {
         log.info(server.getMBeanInfo(galderOn));
         assert false : "Failure expected, " + galderOn + " shouldn't be registered in mbean server";
      } catch (InstanceNotFoundException e) {
      }
   }


   public void testDuplicateJmxDomainOnlyCacheExposesJmxStatistics() throws Exception {
      CacheContainer otherContainer = null;
      try {
         otherContainer = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(JMX_DOMAIN, false, true);
         otherContainer.getCache();
         assert false : "Failure expected, " + JMX_DOMAIN + " is a duplicate!";
      } catch (CacheException e) {
         assert e instanceof JmxDomainConflictException;
      } finally {
         TestingUtil.killCacheManagers(otherContainer);
      }
   }

   public void testMalformedCacheName(Method m) throws Exception {
      final String otherJmxDomain = JMX_DOMAIN + '.' + m.getName();
      CacheContainer otherContainer = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(otherJmxDomain);
      try {
         otherContainer.getCache("persistence.unit:unitName=#helloworld.MyRegion");
      } finally {
         otherContainer.stop();
      }
   }
}
