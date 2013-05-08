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

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tester class for {@link ComponentsJmxRegistration}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "jmx.ComponentsJmxRegistrationTest")
public class ComponentsJmxRegistrationTest extends AbstractInfinispanTest {
   public static final String JMX_DOMAIN = ComponentsJmxRegistrationTest.class.getSimpleName();
   private MBeanServer mBeanServer;
   private List<EmbeddedCacheManager> cacheContainers = new ArrayList<EmbeddedCacheManager>();

   @BeforeMethod
   public void setUp() {
      mBeanServer = MBeanServerFactory.createMBeanServer();
      cacheContainers.clear();
   }

   @AfterMethod
   public void tearDown() {
      MBeanServerFactory.releaseMBeanServer(mBeanServer);
      TestingUtil.killCacheManagers(cacheContainers);
      cacheContainers.clear();
   }

   public void testStopStartCM() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      cacheContainers.add(cm);
      cm.stop();
      cm.start();
   }

   public void testRegisterLocalCache() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      cacheContainers.add(cm);
      cm.start();
      Configuration configuration = config();
      configuration.setCacheMode(Configuration.CacheMode.LOCAL);
      cm.defineConfiguration("first", configuration);
      Cache first = cm.getCache("first");

      ComponentsJmxRegistration regComponents = buildRegistrator(first);
      regComponents.registerMBeans();
      String name = regComponents.getObjectName("Statistics").toString();
      ObjectName name1 = new ObjectName(name);
      assert mBeanServer.isRegistered(name1);
      regComponents.unregisterMBeans();
      assert !mBeanServer.isRegistered(name1);
      assertCorrectJmxName(name1, first);
   }

   private ComponentsJmxRegistration buildRegistrator(Cache cache) {
      AdvancedCache ac = (AdvancedCache) cache;
      Set<AbstractComponentRegistry.Component> components = ac.getComponentRegistry().getRegisteredComponents();
      String groupName = "name=" + ObjectName.quote(cache.getName());
      ComponentsJmxRegistration registrator = new ComponentsJmxRegistration(mBeanServer, components, groupName);
      registrator.setJmxDomain(JMX_DOMAIN);
      return registrator;
   }

   public void testRegisterReplicatedCache() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setAllowDuplicateDomains(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      cacheContainers.add(cm);
      cm.start();
      Configuration configurationOverride = config();
      configurationOverride.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration("first", configurationOverride);
      Cache first = cm.getCache("first");

      ComponentsJmxRegistration regComponents = buildRegistrator(first);
      regComponents.registerMBeans();
      String name = regComponents.getObjectName("Statistics").toString();
      ObjectName name1 = new ObjectName(name);
      assertCorrectJmxName(name1, first);
      assert mBeanServer.isRegistered(name1);
      regComponents.unregisterMBeans();
      assert !mBeanServer.isRegistered(name1);
   }

   public void testLocalAndReplicatedCache() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setAllowDuplicateDomains(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalConfiguration);
      cacheContainers.add(cm);
      cm.start();
      Configuration replicated = config();
      Configuration local = config();
      replicated.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      local.setCacheMode(Configuration.CacheMode.LOCAL);
      cm.defineConfiguration("replicated", replicated);
      cm.defineConfiguration("local", local);
      Cache replicatedCache = cm.getCache("replicated");
      Cache localCache = cm.getCache("local");

      ComponentsJmxRegistration replicatedRegComponents = buildRegistrator(replicatedCache);
      ComponentsJmxRegistration localRegComponents = buildRegistrator(localCache);
      replicatedRegComponents.registerMBeans();
      localRegComponents.registerMBeans();

      String replicatedtCMgmtIntName = replicatedRegComponents.getObjectName("Statistics").toString();
      String localCMgmtIntName = localRegComponents.getObjectName("Statistics").toString();
      ObjectName replObjectName = new ObjectName(replicatedtCMgmtIntName);
      ObjectName localObjName = new ObjectName(localCMgmtIntName);
      assertCorrectJmxName(replObjectName, replicatedCache);

      assert mBeanServer.isRegistered(replObjectName);
      assert mBeanServer.isRegistered(localObjName);
      assert !localCMgmtIntName.equals(replicatedtCMgmtIntName);

      replicatedRegComponents.unregisterMBeans();
      localRegComponents.unregisterMBeans();
      assert !mBeanServer.isRegistered(new ObjectName(localCMgmtIntName));
      assert !mBeanServer.isRegistered(new ObjectName(replicatedtCMgmtIntName));
   }

   private void assertCorrectJmxName(ObjectName objectName, Cache cache) {
      assert ObjectName.unquote(objectName.getKeyProperty(ComponentsJmxRegistration.NAME_KEY)).startsWith(cache.getName());
      assert objectName.getKeyProperty(ComponentsJmxRegistration.COMPONENT_KEY) != null;
   }

   private Configuration config() {
      Configuration configuration = new Configuration();
      configuration.setFetchInMemoryState(false);
      configuration.setExposeJmxStatistics(true);
      return configuration;
   }
}
