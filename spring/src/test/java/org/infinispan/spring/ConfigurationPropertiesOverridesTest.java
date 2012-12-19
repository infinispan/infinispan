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

package org.infinispan.spring;

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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link ConfigurationPropertiesOverrides}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * 
 */
@Test(groups = "unit", testName = "spring.ConfigurationPropertiesOverridesTest")
public class ConfigurationPropertiesOverridesTest {

   private final Properties defaultConfigurationProperties = new ConfigurationProperties()
            .getProperties();

   /**
    * Test method for {@link org.infinispan.spring.ConfigurationPropertiesOverrides#isEmpty()}.
    */
   @Test
   public final void isEmptyShouldRecognizeThatConfigurationPropertiesOverridesAreEmpty() {
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      assertTrue(
               "isEmpty() should have noticed that the ConfigurationPropertiesOverrides instance is indeed empty. However, it didn't.",
               objectUnderTest.isEmpty());
   }

   /**
    * Test method for {@link org.infinispan.spring.ConfigurationPropertiesOverrides#isEmpty()}.
    */
   @Test
   public final void isEmptyShouldRecognizeThatConfigurationPropertiesOverridesAreNotEmpty() {
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();
      objectUnderTest.setTransportFactory("test.TransportFactory");

      assertFalse(
               "isEmpty() should have noticed that the ConfigurationPropertiesOverrides instance is not empty. However, it didn't.",
               objectUnderTest.isEmpty());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setTransportFactory(java.lang.String)}
    * .
    */
   @Test
   public final void setTransportFactoryShouldOverrideDefaultTransportFactory() {
      final String expectedTransportFactory = "test.TransportFactory";
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setTransportFactory(expectedTransportFactory);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               expectedTransportFactory,
               overriddenConfigurationProperties.getProperty(TRANSPORT_FACTORY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setServerList(java.util.Collection)}
    * .
    */
   @Test
   public final void setServerListShouldOverrideDefaultServerList() {
      final Collection<InetSocketAddress> expectedServerList = new ArrayList<InetSocketAddress>(1);
      expectedServerList.add(new InetSocketAddress("testhost", 4632));
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();
      final String expectedServerListString = "testhost:4632";

      objectUnderTest.setServerList(expectedServerList);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               expectedServerListString, overriddenConfigurationProperties.getProperty(SERVER_LIST));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setMarshaller(java.lang.String)}
    * .
    */
   @Test
   public final void setMarshallerShouldOverrideDefaultMarshaller() {
      final String expectedMarshaller = "test.Marshaller";
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setMarshaller(expectedMarshaller);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               expectedMarshaller, overriddenConfigurationProperties.getProperty(MARSHALLER));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setAsyncExecutorFactory(java.lang.String)}
    * .
    */
   @Test
   public final void setAsyncExecutorFactoryShouldOverrideDefaultAsyncExecutorFactory() {
      final String expectedAsyncExecutorFactory = "test.AsyncExecutorFactor";
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setAsyncExecutorFactory(expectedAsyncExecutorFactory);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               expectedAsyncExecutorFactory,
               overriddenConfigurationProperties.getProperty(ASYNC_EXECUTOR_FACTORY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setTcpNoDelay(boolean)}.
    */
   @Test
   public final void setTcpNoDelayShouldOverrideDefaultTcpNoDelay() {
      final boolean expectedTcpNoDelay = true;
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setTcpNoDelay(expectedTcpNoDelay);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               String.valueOf(expectedTcpNoDelay),
               overriddenConfigurationProperties.getProperty(TCP_NO_DELAY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setPingOnStartup(boolean)}.
    */
   @Test
   public final void setPingOnStartupShouldOverrideDefaultPingOnStartup() {
      final boolean expectedPingOnStartup = true;
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setPingOnStartup(expectedPingOnStartup);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               String.valueOf(expectedPingOnStartup),
               overriddenConfigurationProperties.getProperty(PING_ON_STARTUP));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setRequestBalancingStrategy(java.lang.String)}
    * .
    */
   @Test
   public final void setRequestBalancingStrategyShouldOverrideDefaultRequestBalancingStrategy() {
      final String expectedRequestBalancingStrategy = "test.RequestBalancingStrategy";
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setRequestBalancingStrategy(expectedRequestBalancingStrategy);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               expectedRequestBalancingStrategy,
               overriddenConfigurationProperties.getProperty(REQUEST_BALANCING_STRATEGY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setKeySizeEstimate(int)}.
    */
   @Test
   public final void setKeySizeEstimateShouldOverrideDefaultKeySizeEstimate() {
      final int expectedKeySizeEstimate = -123456;
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setKeySizeEstimate(expectedKeySizeEstimate);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               String.valueOf(expectedKeySizeEstimate),
               overriddenConfigurationProperties.getProperty(KEY_SIZE_ESTIMATE));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setValueSizeEstimate(int)}.
    */
   @Test
   public final void setValueSizeEstimateShouldOverrideDefaultValueSizeEstimate() {
      final int expectedValueSizeEstimate = -3456789;
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setValueSizeEstimate(expectedValueSizeEstimate);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               String.valueOf(expectedValueSizeEstimate),
               overriddenConfigurationProperties.getProperty(VALUE_SIZE_ESTIMATE));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.ConfigurationPropertiesOverrides#setForceReturnValues(boolean)}.
    */
   @Test
   public final void setForceReturnValuesShouldOverrideDefaultForceReturnValues() {
      final boolean expectedForceReturnValues = true;
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setForceReturnValues(expectedForceReturnValues);
      final Properties overriddenConfigurationProperties = objectUnderTest
               .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
               + ") should have overridden property 'transportFactory'. However, it didn't.",
               String.valueOf(expectedForceReturnValues),
               overriddenConfigurationProperties.getProperty(FORCE_RETURN_VALUES));
   }
}
