package org.infinispan.spring.remote;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
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
 * @author Olaf Bergner
 *
 */
@Test(groups = "unit", testName = "spring.ConfigurationPropertiesOverridesTest")
public class ConfigurationPropertiesOverridesTest {

   private final Properties defaultConfigurationProperties = new ConfigurationProperties()
         .getProperties();

   @Test
   public final void testIfIsEmptyRecognizesThatConfigurationPropertiesOverridesAreEmpty() {
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      assertTrue(
            "isEmpty() should have noticed that the ConfigurationPropertiesOverrides instance is indeed empty. However, it didn't.",
            objectUnderTest.isEmpty());
   }

   @Test
   public final void testIfIsEmptyShouldRecognizesThatConfigurationPropertiesOverridesAreNotEmpty() {
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();
      objectUnderTest.setMarshaller("test.Marshaller");

      assertFalse(
            "isEmpty() should have noticed that the ConfigurationPropertiesOverrides instance is not empty. However, it didn't.",
            objectUnderTest.isEmpty());
   }

   @Test
   public final void testIfSetMarshallerOverridesDefaultMarshaller() {
      final String expectedMarshaller = "test.Marshaller";
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setMarshaller(expectedMarshaller);
      final Properties overriddenConfigurationProperties = objectUnderTest
            .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
                         + ") should have overridden property 'transportFactory'. However, it didn't.",
                   expectedMarshaller, overriddenConfigurationProperties.getProperty(MARSHALLER));
   }

   @Test
   public final void testIfSetServerListOverridesDefaultServerList() {
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

   @Test
   public final void testIfSetAsyncExecutorFactoryOverridesDefaultAsyncExecutorFactory() {
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

   @Test
   public final void testIfSetTcpNoDelayOverridesDefaultTcpNoDelay() {
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

   @Test
   public final void testIfSetTcpKeepAliveOverridesDefaultTcpKeepAive() {
      final boolean expectedTcpKeepAlive = false;
      final ConfigurationPropertiesOverrides objectUnderTest = new ConfigurationPropertiesOverrides();

      objectUnderTest.setTcpKeepAlive(expectedTcpKeepAlive);
      final Properties overriddenConfigurationProperties = objectUnderTest
            .override(this.defaultConfigurationProperties);

      assertEquals("override(" + this.defaultConfigurationProperties
                         + ") should have overridden property 'transportFactory'. However, it didn't.",
                   String.valueOf(expectedTcpKeepAlive),
                   overriddenConfigurationProperties.getProperty(TCP_KEEP_ALIVE));
   }

   @Test
   public final void testIfSetRequestBalancingStrategyOverridesDefaultRequestBalancingStrategy() {
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

   @Test
   public final void testIfForceReturnValuesOverridesDefaultForceReturnValues() {
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
