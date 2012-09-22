/**
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
 *   ~
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

package org.infinispan.spring.provider;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Properties;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.ShutdownHookBehavior;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.mock.MockExecutorFatory;
import org.infinispan.spring.mock.MockMarshaller;
import org.infinispan.spring.mock.MockScheduleExecutorFactory;
import org.infinispan.spring.mock.MockTransport;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringEmbeddedCacheManagerFactoryBean}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 * 
 */
@Test(testName = "spring.provider.SpringEmbeddedCacheManagerFactoryBeanTest", groups = "unit")
public class SpringEmbeddedCacheManagerFactoryBeanTest {

   private static final String CACHE_NAME_FROM_CONFIGURATION_FILE = "asyncCache";

   private static final String NAMED_ASYNC_CACHE_CONFIG_LOCATION = "named-async-cache.xml";

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setConfigurationFileLocation(org.springframework.core.io.Resource)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldCreateACacheManagerEvenIfNoDefaultConfigurationLocationHasBeenSet()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertNotNull(
               "getObject() should have returned a valid SpringEmbeddedCacheManager, even if no defaulConfigurationLocation "
                        + "has been specified. However, it returned null.",
               springEmbeddedCacheManager);
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setConfigurationFileLocation(org.springframework.core.io.Resource)}
    * .
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldCreateACustomizedCacheManagerIfGivenADefaultConfigurationLocation()
            throws Exception {
      final Resource infinispanConfig = new ClassPathResource(NAMED_ASYNC_CACHE_CONFIG_LOCATION,
               getClass());

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setConfigurationFileLocation(infinispanConfig);
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();
      assertNotNull(
               "getObject() should have returned a valid SpringEmbeddedCacheManager, configured using the configuration file "
                        + "set on SpringEmbeddedCacheManagerFactoryBean. However, it returned null.",
               springEmbeddedCacheManager);
      final SpringCache cacheDefinedInCustomConfiguration = springEmbeddedCacheManager
               .getCache(CACHE_NAME_FROM_CONFIGURATION_FILE);
      final org.infinispan.configuration.cache.Configuration configuration = ((Cache)cacheDefinedInCustomConfiguration.getNativeCache())
               .getCacheConfiguration();
      assertEquals(
               "The cache named ["
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + "] is configured to have asynchonous replication cache mode. Yet, the cache returned from getCache("
                        + CACHE_NAME_FROM_CONFIGURATION_FILE
                        + ") has a different cache mode. Obviously, SpringEmbeddedCacheManagerFactoryBean did not use "
                        + "the configuration file when instantiating SpringEmbeddedCacheManager.",
               org.infinispan.configuration.cache.CacheMode.REPL_ASYNC,
               configuration.clustering().cacheMode());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#getObjectType()}.
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldReportTheCorrectObjectType()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "getObjectType() should return the most derived class of the actual SpringEmbeddedCacheManager "
                        + "implementation returned from getObject(). However, it didn't.",
               springEmbeddedCacheManager.getClass(), objectUnderTest.getObjectType());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#isSingleton()}.
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldDeclareItselfToOnlyProduceSingletons() {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();

      assertTrue("isSingleton() should always return true. However, it returned false",
               objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#destroy()}.
    * 
    * @throws Exception
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldStopTheCreateEmbeddedCacheManagerWhenBeingDestroyed()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();
      springEmbeddedCacheManager.getCache("default"); // Implicitly starts
                                                      // SpringEmbeddedCacheManager
      objectUnderTest.destroy();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should stop the created SpringEmbeddedCacheManager when being destroyed. "
                        + "However, the created SpringEmbeddedCacheManager is still not terminated.",
               ComponentStatus.TERMINATED, springEmbeddedCacheManager.getNativeCacheManager()
                        .getStatus());
      springEmbeddedCacheManager.stop();
   }

   // ~~~~ Testing overriding setters

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setExposeGlobalJmxStatistics(boolean)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseExposeGlobalJmxStatisticsPropIfExplicitlySet()
            throws Exception {
      final boolean expectedExposeGlobalJmxStatistics = true;

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean() {
         @Override
         protected EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
            return TestCacheManagerFactory.createCacheManager(globalBuilder, builder);
         }
      };
      objectUnderTest.setExposeGlobalJmxStatistics(expectedExposeGlobalJmxStatistics);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set ExposeGlobalJmxStatistics. However, it didn't.",
            expectedExposeGlobalJmxStatistics, springEmbeddedCacheManager
            .getNativeCacheManager().getCacheManagerConfiguration().globalJmxStatistics().enabled());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setJmxDomain(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseJmxDomainPropIfExplicitlySet()
            throws Exception {
      final String expectedJmxDomain = "expected.jmx.Domain";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setJmxDomain(expectedJmxDomain);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set JmxDomain. However, it didn't.",
               expectedJmxDomain, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().globalJmxStatistics().domain());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setMBeanServerProperties(java.util.Properties)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseMBeanServerPropertiesPropIfExplicitlySet()
            throws Exception {
      final Properties expectedMBeanServerProperties = new Properties();
      expectedMBeanServerProperties.setProperty("key", "value");

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setMBeanServerProperties(expectedMBeanServerProperties);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set MBeanServerProperties. However, it didn't.",
               expectedMBeanServerProperties, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().globalJmxStatistics().properties());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setMBeanServerLookupClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseMBeanServerLookupClassPropIfExplicitlySet()
            throws Exception {
      final MBeanServerLookup expectedMBeanServerLookup = new PlatformMBeanServerLookup();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setMBeanServerLookupClass(expectedMBeanServerLookup.getClass().getName());
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set MBeanServerLookupClass. However, it didn't.",
            expectedMBeanServerLookup.getClass().getName(), springEmbeddedCacheManager
            .getNativeCacheManager().getCacheManagerConfiguration().globalJmxStatistics().mbeanServerLookup().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setMBeanServerLookup(org.infinispan.jmx.MBeanServerLookup)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseMBeanServerLookupPropIfExplicitlySet()
            throws Exception {
      final MBeanServerLookup expectedMBeanServerLookup = new PlatformMBeanServerLookup();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setMBeanServerLookup(expectedMBeanServerLookup);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertSame(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set MBeanServerLookup. However, it didn't.",
               expectedMBeanServerLookup.getClass().getName(), springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().globalJmxStatistics().mbeanServerLookup().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setAllowDuplicateDomains(boolean)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseAllowDuplicateDomainsPropIfExplicitlySet()
            throws Exception {
      final boolean expectedAllowDuplicateDomains = true;

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setAllowDuplicateDomains(expectedAllowDuplicateDomains);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set AllowDuplicateDomains. However, it didn't.",
               expectedAllowDuplicateDomains, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().globalJmxStatistics().allowDuplicateDomains());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setCacheManagerName(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseCacheManagerNamePropIfExplicitlySet()
            throws Exception {
      final String expectedCacheManagerName = "expected.cache.manager.Name";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setCacheManagerName(expectedCacheManagerName);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set CacheManagerName. However, it didn't.",
               expectedCacheManagerName, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().globalJmxStatistics().cacheManagerName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setStrictPeerToPeer(boolean)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseStrictPeerToPeerPropIfExplicitlySet()
            throws Exception {
      final boolean expectedStrictPeerToPeer = true;

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setStrictPeerToPeer(expectedStrictPeerToPeer);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set StrictPeerToPeer. However, it didn't.",
               expectedStrictPeerToPeer, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().strictPeerToPeer());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setAsyncListenerExecutorFactoryClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseAsyncListenerExecutorFactoryClassPropIfExplicitlySet()
            throws Exception {
      final String expectedAsyncListenerExecutorFactoryClass = MockExecutorFatory.class.getName();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest
               .setAsyncListenerExecutorFactoryClass(expectedAsyncListenerExecutorFactoryClass);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set AsyncListenerExecutorFactoryClass. However, it didn't.",
               expectedAsyncListenerExecutorFactoryClass, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().asyncListenerExecutor().factory().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setAsyncTransportExecutorFactoryClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseAsyncTransportExecutorFactoryClassPropIfExplicitlySet()
            throws Exception {
      final String expectedAsyncTransportExecutorFactoryClass = MockExecutorFatory.class.getName();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest
               .setAsyncTransportExecutorFactoryClass(expectedAsyncTransportExecutorFactoryClass);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set AsyncTransportExecutorFactoryClass. However, it didn't.",
               expectedAsyncTransportExecutorFactoryClass, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().asyncTransportExecutor().factory().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setEvictionScheduledExecutorFactoryClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseEvictionScheduledExecutorFactoryClassPropIfExplicitlySet()
            throws Exception {
      final String expectedEvictionScheduledExecutorFactoryClass = MockScheduleExecutorFactory.class
               .getName();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest
               .setEvictionScheduledExecutorFactoryClass(expectedEvictionScheduledExecutorFactoryClass);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set EvictionScheduledExecutorFactoryClass. However, it didn't.",
               expectedEvictionScheduledExecutorFactoryClass, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().evictionScheduledExecutor().factory().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setReplicationQueueScheduledExecutorFactoryClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseReplicationQueueScheduledExecutorFactoryClassPropIfExplicitlySet()
            throws Exception {
      final String expectedReplicationQueueScheduledExecutorFactoryClass = MockScheduleExecutorFactory.class
               .getName();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest
               .setReplicationQueueScheduledExecutorFactoryClass(expectedReplicationQueueScheduledExecutorFactoryClass);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set ReplicationQueueScheduledExecutorFactoryClass. However, it didn't.",
               expectedReplicationQueueScheduledExecutorFactoryClass, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().replicationQueueScheduledExecutor().factory().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setMarshallerClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseMarshallerClassPropIfExplicitlySet()
            throws Exception {
      final String expectedMarshallerClass = MockMarshaller.class.getName();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setMarshallerClass(expectedMarshallerClass);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set MarshallerClass. However, it didn't.",
            expectedMarshallerClass, springEmbeddedCacheManager.getNativeCacheManager()
            .getCacheManagerConfiguration().serialization().marshaller().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setTransportNodeName(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseTransportNodeNamePropIfExplicitlySet()
            throws Exception {
      final String expectedTransportNodeName = "expected.transport.node.Name";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setTransportNodeName(expectedTransportNodeName);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
            "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set TransportNodeName. However, it didn't.",
            expectedTransportNodeName, springEmbeddedCacheManager.getNativeCacheManager()
            .getCacheManagerConfiguration().transport().nodeName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setTransportClass(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseTransportClassPropIfExplicitlySet()
            throws Exception {
      final String expectedTransportClass = MockTransport.class.getName();

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setTransportClass(expectedTransportClass);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set TransportClass. However, it didn't.",
               expectedTransportClass, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().transport().getClass().getName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setTransportProperties(java.util.Properties)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseTransportPropertiesPropIfExplicitlySet()
            throws Exception {
      final Properties expectedTransportProperties = new Properties();
      expectedTransportProperties.setProperty("key", "value");

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setTransportProperties(expectedTransportProperties);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set TransportProperties. However, it didn't.",
               expectedTransportProperties, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().properties());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setClusterName(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseClusterNamePropIfExplicitlySet()
            throws Exception {
      final String expectedClusterName = "expected.cluster.Name";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setClusterName(expectedClusterName);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set ClusterName. However, it didn't.",
               expectedClusterName, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().clusterName());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setMachineId(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseMachineIdPropIfExplicitlySet()
            throws Exception {
      final String expectedMachineId = "expected.machine.Id";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setMachineId(expectedMachineId);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set MachineId. However, it didn't.",
               expectedMachineId, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().machineId());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setRackId(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseRackIdPropIfExplicitlySet()
            throws Exception {
      final String expectedRackId = "expected.rack.Id";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setRackId(expectedRackId);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set RackId. However, it didn't.",
               expectedRackId, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().rackId());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setSiteId(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseSiteIdPropIfExplicitlySet()
            throws Exception {
      final String expectedSiteId = "expected.site.Id";

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setSiteId(expectedSiteId);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set SiteId. However, it didn't.",
               expectedSiteId, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().siteId());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setShutdownHookBehavior(java.lang.String)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseShutdownHookBehaviorPropIfExplicitlySet()
            throws Exception {
      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setShutdownHookBehavior(ShutdownHookBehavior.DONT_REGISTER.name());
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set ShutdownHookBehavior. However, it didn't.",
               ShutdownHookBehavior.DONT_REGISTER, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().shutdown().hookBehavior());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setAsyncListenerExecutorProperties(java.util.Properties)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseAsyncListenerExecutorPropertiesPropIfExplicitlySet()
            throws Exception {
      final Properties expectedAsyncListenerExecutorProperties = new Properties();
      expectedAsyncListenerExecutorProperties.setProperty("key", "value");

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setAsyncListenerExecutorProperties(expectedAsyncListenerExecutorProperties);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set AsyncListenerExecutorProperties. However, it didn't.",
               expectedAsyncListenerExecutorProperties, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().asyncListenerExecutor().properties());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setAsyncTransportExecutorProperties(java.util.Properties)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseAsyncTransportExecutorPropertiesPropIfExplicitlySet()
            throws Exception {
      final Properties expectedAsyncTransportExecutorProperties = new Properties();
      expectedAsyncTransportExecutorProperties.setProperty("key", "value");

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setAsyncTransportExecutorProperties(expectedAsyncTransportExecutorProperties);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set AsyncTransportExecutorProperties. However, it didn't.",
               expectedAsyncTransportExecutorProperties, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().asyncTransportExecutor().properties());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setEvictionScheduledExecutorProperties(java.util.Properties)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseEvictionScheduledExecutorPropertiesPropIfExplicitlySet()
            throws Exception {
      final Properties expectedEvictionScheduledExecutorProperties = new Properties();
      expectedEvictionScheduledExecutorProperties.setProperty("key", "value");

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest
               .setEvictionScheduledExecutorProperties(expectedEvictionScheduledExecutorProperties);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set EvictionScheduledExecutorProperties. However, it didn't.",
               expectedEvictionScheduledExecutorProperties, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().evictionScheduledExecutor().properties());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setReplicationQueueScheduledExecutorProperties(java.util.Properties)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseReplicationQueueScheduledExecutorPropertiesPropIfExplicitlySet()
            throws Exception {
      final Properties expectedReplicationQueueScheduledExecutorProperties = new Properties();
      expectedReplicationQueueScheduledExecutorProperties.setProperty("key", "value");

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest
               .setReplicationQueueScheduledExecutorProperties(expectedReplicationQueueScheduledExecutorProperties);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set ReplicationQueueScheduledExecutorProperties. However, it didn't.",
               expectedReplicationQueueScheduledExecutorProperties, springEmbeddedCacheManager
                        .getNativeCacheManager().getCacheManagerConfiguration().replicationQueueScheduledExecutor().properties());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setMarshallVersion(short)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseMarshallVersionPropIfExplicitlySet()
            throws Exception {
      final short setMarshallVersion = 1234;

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setMarshallVersion(setMarshallVersion);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set MarshallVersion. However, it didn't.",
               setMarshallVersion, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().serialization().version());
      springEmbeddedCacheManager.stop();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringEmbeddedCacheManagerFactoryBean#setDistributedSyncTimeout(long)}
    * .
    */
   @Test
   public final void springEmbeddedCacheManagerFactoryBeanShouldUseDistributedSyncTimeoutPropIfExplicitlySet()
            throws Exception {
      final long expectedDistributedSyncTimeout = 123456L;

      final SpringEmbeddedCacheManagerFactoryBean objectUnderTest = new SpringEmbeddedCacheManagerFactoryBean();
      objectUnderTest.setDistributedSyncTimeout(expectedDistributedSyncTimeout);
      objectUnderTest.afterPropertiesSet();
      final SpringEmbeddedCacheManager springEmbeddedCacheManager = objectUnderTest.getObject();

      assertEquals(
               "SpringEmbeddedCacheManagerFactoryBean should have used explicitly set DistributedSyncTimeout. However, it didn't.",
               expectedDistributedSyncTimeout, springEmbeddedCacheManager.getNativeCacheManager()
                        .getCacheManagerConfiguration().transport().distributedSyncTimeout());
      springEmbeddedCacheManager.stop();
   }
}
