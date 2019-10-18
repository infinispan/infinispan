package org.infinispan.spring.remote.provider;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JAVA_SERIAL_WHITELIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_SIZE_ESTIMATE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.VALUE_SIZE_ESTIMATE;
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
import org.infinispan.client.hotrod.SomeRequestBalancingStrategy;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.spring.remote.AssertionUtils;
import org.infinispan.test.AbstractInfinispanTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link SpringRemoteCacheManagerFactoryBean}.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 *
 */
@Test(testName = "spring.remote.provider.SpringRemoteCacheManagerFactoryBeanTest", groups = "unit")
public class SpringRemoteCacheManagerFactoryBeanTest extends AbstractInfinispanTest {

   private static final Resource HOTROD_CLIENT_PROPERTIES_LOCATION = new ClassPathResource(
         "hotrod-client.properties", SpringRemoteCacheManagerFactoryBeanTest.class);
   private SpringRemoteCacheManagerFactoryBean objectUnderTest;

   @AfterMethod(alwaysRun = true)
   public void afterMethod() throws Exception {
      if (objectUnderTest != null) {
         objectUnderTest.destroy();
      }
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void shouldThrowAnIllegalStateExceptionIfBothConfigurationPropertiesAndConfifurationPropertiesFileLocationAreSet()
         throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationProperties(new Properties());
      objectUnderTest.setConfigurationPropertiesFileLocation(new ClassPathResource("dummy", getClass()));

      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void shouldThrowAnIllegalStateExceptionIfConfigurationPropertiesAsWellAsSettersAreUsedToConfigureTheRemoteCacheManager()
         throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationProperties(new Properties());
      objectUnderTest.setMarshaller("test.Marshaller");

      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#getObjectType()}.
    *
    * @throws Exception
    */
   @Test
   public final void infinispanRemoteCacheFactoryBeanShouldReportTheMostDerivedObjectType()
         throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      assertEquals(
            "getObjectType() should have returned the most derived class of the actual RemoteCache "
                  + "implementation returned from getObject(). However, it didn't.",
            objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#getObject()}.
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceARemoteCacheManagerConfiguredUsingDefaultSettingsIfNeitherConfigurationPropertiesNorConfigurationPropertiesFileLocationHasBeenSet()
         throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();

      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      RemoteCacheManager defaultRemoteCacheManager = new RemoteCacheManager();

      // Explicitly set the expected properties on the client defaults, as otherwise the ProtoStream marshaller is expected
      Properties clientDefaultProps = defaultRemoteCacheManager.getConfiguration().properties();
      clientDefaultProps.setProperty(MARSHALLER, JavaSerializationMarshaller.class.getName());
      clientDefaultProps.setProperty(JAVA_SERIAL_WHITELIST, SpringRemoteCacheManagerFactoryBean.SPRING_JAVA_SERIAL_WHITELIST);

      try {
         AssertionUtils.assertPropertiesSubset(
                 "The configuration properties used by the SpringRemoteCacheManager returned von getObject() should be equal "
                         + "to SpringRemoteCacheManager's default settings since neither property 'configurationProperties' "
                         + "nor property 'configurationPropertiesFileLocation' has been set. However, those two are not equal.",
                 clientDefaultProps,
               remoteCacheManager.getNativeCacheManager().getConfiguration().properties());
      } finally {
         defaultRemoteCacheManager.stop();
      }
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#isSingleton()}.
    */
   @Test
   public final void isSingletonShouldAlwaysReturnTrue() {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();

      assertTrue(
            "isSingleton() should always return true since each SpringRemoteCacheManagerFactoryBean will always produce "
                  + "the same SpringRemoteCacheManager instance. However,it returned false.",
            objectUnderTest.isSingleton());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#destroy()}.
    *
    * @throws Exception
    */
   @Test
   public final void destroyShouldStopTheProducedCache() throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
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
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setConfigurationProperties(Properties)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceACacheConfiguredUsingTheSuppliedConfigurationProperties()
         throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      final Properties configurationProperties = loadConfigurationProperties(HOTROD_CLIENT_PROPERTIES_LOCATION);
      objectUnderTest.setConfigurationProperties(configurationProperties);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      AssertionUtils.assertPropertiesSubset(
              "The configuration properties used by the SpringRemoteCacheManager returned von getObject() should be equal "
                      + "to those passed into SpringRemoteCacheManagerFactoryBean via setConfigurationProperties(props). "
                      + "However, those two are not equal.", configurationProperties,
              remoteCacheManager.getNativeCacheManager().getConfiguration().properties());
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
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setConfigurationPropertiesFileLocation(Resource)}
    * .
    */
   @Test
   public final void shouldProduceACacheConfiguredUsingPropertiesLoadedFromALocationDeclaredThroughSetConfigurationPropertiesFileLocation()
         throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationPropertiesFileLocation(HOTROD_CLIENT_PROPERTIES_LOCATION);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      AssertionUtils.assertPropertiesSubset(
              "The configuration properties used by the SpringRemoteCacheManager returned von getObject() should be equal "
                      + "to those passed into SpringRemoteCacheManagerFactoryBean via setConfigurationPropertiesFileLocation(propsFileLocation). "
                      + "However, those two are not equal.",
              loadConfigurationProperties(HOTROD_CLIENT_PROPERTIES_LOCATION), remoteCacheManager
                      .getNativeCacheManager().getConfiguration().properties());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setStartAutomatically(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceAStoppedCacheIfStartAutomaticallyIsSetToFalse() throws Exception {
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManagerExpectedToBeInStateStopped = objectUnderTest
            .getObject();

      assertFalse(
            "SpringRemoteCacheManagerFactoryBean should have produced a SpringRemoteCacheManager that is initially in state stopped "
                  + "since property 'startAutomatically' has been set to false. However, the produced SpringRemoteCacheManager is already started.",
            remoteCacheManagerExpectedToBeInStateStopped.getNativeCacheManager().isStarted());
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setServerList(Collection)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setServerListShouldOverrideDefaultServerList() throws Exception {
      final Collection<InetSocketAddress> expectedServerList = new ArrayList<InetSocketAddress>(1);
      expectedServerList.add(new InetSocketAddress("testhost", 4632));
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      final String expectedServerListString = "testhost:4632";
      objectUnderTest.setServerList(expectedServerList);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setServerList(" + expectedServerList
                         + ") should have overridden property 'serverList'. However, it didn't.",
                   expectedServerListString, remoteCacheManager.getNativeCacheManager().getConfiguration().properties()
                  .get(SERVER_LIST));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setMarshaller(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setMarshallerShouldOverrideDefaultMarshaller() throws Exception {
      final String expectedMarshaller = Marshaller.class.getName();
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setMarshaller(expectedMarshaller);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setMarshaller(" + expectedMarshaller
                         + ") should have overridden property 'marshaller'. However, it didn't.",
                   expectedMarshaller,
                   remoteCacheManager.getNativeCacheManager().getConfiguration().properties().get(MARSHALLER));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setAsyncExecutorFactory(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setAsyncExecutorFactoryShouldOverrideDefaultAsyncExecutorFactory()
         throws Exception {
      final String expectedAsyncExecutorFactory = ExecutorFactory.class.getName();
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setAsyncExecutorFactory(expectedAsyncExecutorFactory);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setAsyncExecutorFactory(" + expectedAsyncExecutorFactory
                         + ") should have overridden property 'asyncExecutorFactory'. However, it didn't.",
                   expectedAsyncExecutorFactory, remoteCacheManager.getNativeCacheManager()
                  .getConfiguration().properties().get(ASYNC_EXECUTOR_FACTORY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setTcpNoDelay(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setTcpNoDelayShouldOverrideDefaultTcpNoDelay() throws Exception {
      final boolean expectedTcpNoDelay = true;
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setTcpNoDelay(expectedTcpNoDelay);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setTcpNoDelay(" + expectedTcpNoDelay
                         + ") should have overridden property 'tcpNoDelay'. However, it didn't.",
                   String.valueOf(expectedTcpNoDelay), remoteCacheManager.getNativeCacheManager()
                  .getConfiguration().properties().get(TCP_NO_DELAY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setTcpNoDelay(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setTcpKeepAliveOverrideDefaultTcpKeepAlive() throws Exception {
      final boolean expectedTcpKeepAlive = false;
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setTcpKeepAlive(expectedTcpKeepAlive);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setTcpKeepAlive(" + expectedTcpKeepAlive
                         + ") should have overridden property 'tcpKeepAlive'. However, it didn't.",
                   String.valueOf(expectedTcpKeepAlive), remoteCacheManager.getNativeCacheManager()
                  .getConfiguration().properties().get(TCP_KEEP_ALIVE));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setRequestBalancingStrategy(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setRequestBalancingStrategyShouldOverrideDefaultRequestBalancingStrategy()
         throws Exception {
      final String expectedRequestBalancingStrategy = SomeRequestBalancingStrategy.class.getName();
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setRequestBalancingStrategy(expectedRequestBalancingStrategy);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(
            "setRequestBalancingStrategy("
                  + expectedRequestBalancingStrategy
                  + ") should have overridden property 'requestBalancingStrategy'. However, it didn't.",
            expectedRequestBalancingStrategy, remoteCacheManager.getNativeCacheManager()
                  .getConfiguration().properties().get(REQUEST_BALANCING_STRATEGY));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setKeySizeEstimate(int)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setKeySizeEstimateShouldOverrideDefaultKeySizeEstimate() throws Exception {
      final int expectedKeySizeEstimate = -123456;
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setKeySizeEstimate(expectedKeySizeEstimate);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setKeySizeEstimate(" + expectedKeySizeEstimate
                         + ") should have overridden property 'keySizeEstimate'. However, it didn't.",
                   String.valueOf(expectedKeySizeEstimate), remoteCacheManager.getNativeCacheManager()
                  .getConfiguration().properties().get(KEY_SIZE_ESTIMATE));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setValueSizeEstimate(int)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setValueSizeEstimateShouldOverrideDefaultValueSizeEstimate() throws Exception {
      final int expectedValueSizeEstimate = -3456789;
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setValueSizeEstimate(expectedValueSizeEstimate);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setValueSizeEstimate(" + expectedValueSizeEstimate
                         + ") should have overridden property 'valueSizeEstimate'. However, it didn't.",
                   String.valueOf(expectedValueSizeEstimate), remoteCacheManager
                  .getNativeCacheManager().getConfiguration().properties().get(VALUE_SIZE_ESTIMATE));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setForceReturnValues(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setForceReturnValuesShouldOverrideDefaultForceReturnValues() throws Exception {
      final boolean expectedForceReturnValues = true;
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setForceReturnValues(expectedForceReturnValues);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setForceReturnValue(" + expectedForceReturnValues
                         + ") should have overridden property 'forceReturnValue'. However, it didn't.",
                   String.valueOf(expectedForceReturnValues), remoteCacheManager
                  .getNativeCacheManager().getConfiguration().properties().get(FORCE_RETURN_VALUES));
   }

   /**
    * Test method for
    * {@link org.infinispan.spring.remote.provider.SpringRemoteCacheManagerFactoryBean#setForceReturnValues(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setReadTimeoutShouldOverrideDefaultReadTimeout() throws Exception {
      final long expectedReadTimeout = 500;
      objectUnderTest = new SpringRemoteCacheManagerFactoryBean();
      objectUnderTest.setReadTimeout(expectedReadTimeout);
      objectUnderTest.afterPropertiesSet();

      final SpringRemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals("setReadTimeout(" + expectedReadTimeout
                  + ") should have overridden property 'readTimeout'. However, it didn't.",
            expectedReadTimeout, remoteCacheManager.getReadTimeout());
   }
}
