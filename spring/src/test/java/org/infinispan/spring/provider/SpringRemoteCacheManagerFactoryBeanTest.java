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

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_SIZE_ESTIMATE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.PING_ON_STARTUP;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSPORT_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.VALUE_SIZE_ESTIMATE;
import static org.infinispan.spring.AssertionUtils.assertPropertiesSubset;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCacheManager;

import org.infinispan.spring.mock.MockExecutorFatory;
import org.infinispan.spring.mock.MockMarshaller;
import org.infinispan.spring.mock.MockRequestBalancingStrategy;
import org.infinispan.spring.mock.MockTransportFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringRemoteCacheManagerFactoryBean}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
@Test(testName = "spring.provider.SpringRemoteCacheManagerFactoryBeanTest", groups = "unit")
public class SpringRemoteCacheManagerFactoryBeanTest {

   private static final Resource HOTROD_CLIENT_PROPERTIES_LOCATION = new ClassPathResource(
            "hotrod-client.properties", SpringRemoteCacheManagerFactoryBeanTest.class);

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void shouldThrowAnIllegalStateExceptionIfBothConfigurationPropertiesAndConfifurationPropertiesFileLocationAreSet()
            throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationProperties(new Properties());
      objectUnderTest.setConfigurationPropertiesFileLocation(new ClassPathResource("dummy",
               getClass()));

      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void shouldThrowAnIllegalStateExceptionIfConfigurationPropertiesAsWellAsSettersAreUsedToConfigureTheRemoteCacheManager()
            throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationProperties(new Properties());
      objectUnderTest.setTransportFactory("test.TransportFactory");

      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#getObjectType()}.
    *
    * @throws Exception
    */
   @Test
   public final void infinispanRemoteCacheFactoryBeanShouldReportTheMostDerivedObjectType()
            throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      assertEquals(
               "getObjectType() should have returned the most derived class of the actual RemoteCache "
                        + "implementation returned from getObject(). However, it didn't.",
               objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#getObject()}.
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceARemoteCacheManagerConfiguredUsingDefaultSettingsIfNeitherConfigurationPropertiesNorConfigurationPropertiesFileLocationHasBeenSet()
            throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();

      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      assertPropertiesSubset(
               "The configuration properties used by the SpringRemoteCacheManager returned von getObject() should be equal "
                        + "to SpringRemoteCacheManager's default settings since neither property 'configurationProperties' "
                        + "nor property 'configurationPropertiesFileLocation' has been set. However, those two are not equal.",
               new RemoteCacheManager().getProperties(), remoteCacheManager.getNativeCacheManager()
                        .getProperties());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#isSingleton()}.
    */
   @Test
   public final void isSingletonShouldAlwaysReturnTrue() {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();

      assertTrue(
               "isSingleton() should always return true since each SpringRemoteCacheManagerFactoryBean will always produce "
                        + "the same SpringRemoteCacheManager instance. However,it returned false.",
               objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#destroy()}.
    *
    * @throws Exception
    */
   @Test
   public final void destroyShouldStopTheProducedCache() throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();
      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      objectUnderTest.destroy();

      assertFalse(
               "destroy() should have stopped the SpringRemoteCacheManager instance previously produced by "
                        + "SpringRemoteCacheManagerFactoryBean. However, the produced SpringRemoteCacheManager is still running. ",
               remoteCacheManager.getNativeCacheManager().isStarted());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setConfigurationProperties(java.util.Properties)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceACacheConfiguredUsingTheSuppliedConfigurationProperties()
            throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      final Properties configurationProperties = loadConfigurationProperties(HOTROD_CLIENT_PROPERTIES_LOCATION);
      objectUnderTest.setConfigurationProperties(configurationProperties);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      assertPropertiesSubset(
               "The configuration properties used by the SpringRemoteCacheManager returned von getObject() should be equal "
                        + "to those passed into SpringRemoteCacheManagerFactoryBean via setConfigurationProperties(props). "
                        + "However, those two are not equal.", configurationProperties,
               remoteCacheManager.getNativeCacheManager().getProperties());
      objectUnderTest.destroy();
   }

   private Properties loadConfigurationProperties(final Resource configurationPropertiesLocation)
            throws IOException {
      InputStream propsStream = null;
      try {
         propsStream = HOTROD_CLIENT_PROPERTIES_LOCATION.getInputStream();
         final Properties configurationProperties = new Properties();
         configurationProperties.load(propsStream);

         return configurationProperties;
      } finally {
         if (propsStream != null) {
            propsStream.close();
         }
      }
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setConfigurationPropertiesFileLocation(org.springframework.core.io.Resource)}
    * .
    */
   @Test
   public final void shouldProduceACacheConfiguredUsingPropertiesLoadedFromALocationDeclaredThroughSetConfigurationPropertiesFileLocation()
            throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationPropertiesFileLocation(HOTROD_CLIENT_PROPERTIES_LOCATION);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      assertPropertiesSubset(
               "The configuration properties used by the SpringRemoteCacheManager returned von getObject() should be equal "
                        + "to those passed into SpringRemoteCacheManagerFactoryBean via setConfigurationPropertiesFileLocation(propsFileLocation). "
                        + "However, those two are not equal.",
               loadConfigurationProperties(HOTROD_CLIENT_PROPERTIES_LOCATION), remoteCacheManager
                        .getNativeCacheManager().getProperties());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setStartAutomatically(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceAStoppedCacheIfStartAutomaticallyIsSetToFalse() throws Exception {
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManagerExpectedToBeInStateStopped = objectUnderTest
               .getObject();

      assertFalse(
               "SpringRemoteCacheManagerFactoryBean should have produced a SpringRemoteCacheManager that is initially in state stopped "
                        + "since property 'startAutomatically' has been set to false. However, the produced SpringRemoteCacheManager is already started.",
               remoteCacheManagerExpectedToBeInStateStopped.getNativeCacheManager().isStarted());
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setTransportFactory(java.lang.String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setTransportFactoryShouldOverrideDefaultTransportFactory() throws Exception {
      final String expectedTransportFactory = MockTransportFactory.class.getName();
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setTransportFactory(expectedTransportFactory);
      objectUnderTest.setStartAutomatically(false); // Otherwise, SpringRemoteCacheManager will try
                                                    // to actually use our DummyTransportFactory
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setTransportFactory(" + expectedTransportFactory
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               expectedTransportFactory, remoteCacheManager.getNativeCacheManager().getProperties()
                        .get(TRANSPORT_FACTORY));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setServerList(java.util.Collection)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setServerListShouldOverrideDefaultServerList() throws Exception {
      final Collection<InetSocketAddress> expectedServerList = new ArrayList<InetSocketAddress>(1);
      expectedServerList.add(new InetSocketAddress("testhost", 4632));
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      final String expectedServerListString = "testhost:4632";
      objectUnderTest.setServerList(expectedServerList);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setServerList(" + expectedServerList
               + ") should have overridden property 'serverList'. However, it didn't.",
               expectedServerListString, remoteCacheManager.getNativeCacheManager().getProperties()
                        .get(SERVER_LIST));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setMarshaller(java.lang.String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setMarshallerShouldOverrideDefaultMarshaller() throws Exception {
      final String expectedMarshaller = MockMarshaller.class.getName();
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setMarshaller(expectedMarshaller);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setMarshaller(" + expectedMarshaller
               + ") should have overridden property 'marshaller'. However, it didn't.",
               expectedMarshaller,
               remoteCacheManager.getNativeCacheManager().getProperties().get(MARSHALLER));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setAsyncExecutorFactory(java.lang.String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setAsyncExecutorFactoryShouldOverrideDefaultAsyncExecutorFactory()
            throws Exception {
      final String expectedAsyncExecutorFactory = MockExecutorFatory.class.getName();
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setAsyncExecutorFactory(expectedAsyncExecutorFactory);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setAsyncExecutorFactory(" + expectedAsyncExecutorFactory
               + ") should have overridden property 'asyncExecutorFactory'. However, it didn't.",
               expectedAsyncExecutorFactory, remoteCacheManager.getNativeCacheManager()
                        .getProperties().get(ASYNC_EXECUTOR_FACTORY));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setTcpNoDelay(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setTcpNoDelayShouldOverrideDefaultTcpNoDelay() throws Exception {
      final boolean expectedTcpNoDelay = true;
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setTcpNoDelay(expectedTcpNoDelay);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setTcpNoDelay(" + expectedTcpNoDelay
               + ") should have overridden property 'tcpNoDelay'. However, it didn't.",
               String.valueOf(expectedTcpNoDelay), remoteCacheManager.getNativeCacheManager()
                        .getProperties().get(TCP_NO_DELAY));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setPingOnStartup(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setPingOnStartupShouldOverrideDefaultPingOnStartup() throws Exception {
      final boolean expectedPingOnStartup = true;
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setPingOnStartup(expectedPingOnStartup);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setPingOnStartup(" + expectedPingOnStartup
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               String.valueOf(expectedPingOnStartup), remoteCacheManager.getNativeCacheManager()
                        .getProperties().get(PING_ON_STARTUP));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setRequestBalancingStrategy(java.lang.String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setRequestBalancingStrategyShouldOverrideDefaultRequestBalancingStrategy()
            throws Exception {
      final String expectedRequestBalancingStrategy = MockRequestBalancingStrategy.class.getName();
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setRequestBalancingStrategy(expectedRequestBalancingStrategy);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(
               "setRequestBalancingStrategy("
                        + expectedRequestBalancingStrategy
                        + ") should have overridden property 'requestBalancingStrategy'. However, it didn't.",
               expectedRequestBalancingStrategy, remoteCacheManager.getNativeCacheManager()
                        .getProperties().get(REQUEST_BALANCING_STRATEGY));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setKeySizeEstimate(int)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setKeySizeEstimateShouldOverrideDefaultKeySizeEstimate() throws Exception {
      final int expectedKeySizeEstimate = -123456;
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setKeySizeEstimate(expectedKeySizeEstimate);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setKeySizeEstimate(" + expectedKeySizeEstimate
               + ") should have overridden property 'keySizeEstimate'. However, it didn't.",
               String.valueOf(expectedKeySizeEstimate), remoteCacheManager.getNativeCacheManager()
                        .getProperties().get(KEY_SIZE_ESTIMATE));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setValueSizeEstimate(int)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setValueSizeEstimateShouldOverrideDefaultValueSizeEstimate() throws Exception {
      final int expectedValueSizeEstimate = -3456789;
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setValueSizeEstimate(expectedValueSizeEstimate);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setValueSizeEstimate(" + expectedValueSizeEstimate
               + ") should have overridden property 'valueSizeEstimate'. However, it didn't.",
               String.valueOf(expectedValueSizeEstimate), remoteCacheManager
                        .getNativeCacheManager().getProperties().get(VALUE_SIZE_ESTIMATE));
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.provider.SpringRemoteCacheManagerFactoryBean#setForceReturnValues(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setForceReturnValuesShouldOverrideDefaultForceReturnValues() throws Exception {
      final boolean expectedForceReturnValues = true;
      final SpringRemoteCacheManagerFactoryBean objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setForceReturnValues(expectedForceReturnValues);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setForceReturnValue(" + expectedForceReturnValues
               + ") should have overridden property 'forceReturnValue'. However, it didn't.",
               String.valueOf(expectedForceReturnValues), remoteCacheManager
                        .getNativeCacheManager().getProperties().get(FORCE_RETURN_VALUES));
      objectUnderTest.destroy();
   }
}
