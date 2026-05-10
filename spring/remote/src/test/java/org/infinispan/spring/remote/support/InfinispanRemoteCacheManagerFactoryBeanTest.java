package org.infinispan.spring.remote.support;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JAVA_SERIAL_ALLOWLIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.SomeRequestBalancingStrategy;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.spring.remote.AbstractRemoteCacheManagerFactory;
import org.infinispan.spring.remote.AssertionUtils;
import org.infinispan.test.AbstractInfinispanTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.Test;

/**
 * <p>
 * Test {@link AbstractRemoteCacheManagerFactory}.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
@Test(testName = "spring.remote.support.InfinispanRemoteCacheManagerFactoryBeanTest", groups = "unit")
public class InfinispanRemoteCacheManagerFactoryBeanTest extends AbstractInfinispanTest {

   private static final Resource HOTROD_CLIENT_PROPERTIES_LOCATION = new ClassPathResource(
         "hotrod-client.properties", InfinispanRemoteCacheManagerFactoryBeanTest.class);

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void shouldThrowAnIllegalStateExceptionIfBothConfigurationPropertiesAndConfigurationPropertiesFileLocationAreSet()
         throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationProperties(new Properties());
      objectUnderTest.setConfigurationPropertiesFileLocation(new ClassPathResource("dummy",
            getClass()));

      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#afterPropertiesSet()}
    * .
    *
    * @throws Exception
    */
   @Test(expectedExceptions = IllegalStateException.class)
   public final void shouldThrowAnIllegalStateExceptionIfConfigurationPropertiesAsWellAsSettersAreUsedToConfigureTheRemoteCacheManager()
         throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationProperties(new Properties());
      objectUnderTest.setMarshaller("test.Marshaller");

      objectUnderTest.afterPropertiesSet();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#getObjectType()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void infinispanRemoteCacheFactoryBeanShouldReportTheMostDerivedObjectType()
         throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();

      assertEquals(objectUnderTest.getObject().getClass(), objectUnderTest.getObjectType(),
            "getObjectType() should have returned the most derived class of the actual RemoteCache implementation returned from getObject(). However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#getObject()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceARemoteCacheManagerConfiguredUsingDefaultSettingsIfNeitherConfigurationPropertiesNorConfigurationPropertiesFileLocationHasBeenSet()
         throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();

      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager springRemoteCacheManager = objectUnderTest.getObject();
      RemoteCacheManager defaultRemoteCacheManager = new RemoteCacheManager();

      // Explicitly set the expected properties on the client defaults, as otherwise the ProtoStream marshaller is expected
      Properties clientDefaultProps = defaultRemoteCacheManager.getConfiguration().properties();
      clientDefaultProps.setProperty(MARSHALLER, JavaSerializationMarshaller.class.getName());
      clientDefaultProps.setProperty(JAVA_SERIAL_ALLOWLIST, InfinispanRemoteCacheManagerFactoryBean.SPRING_JAVA_SERIAL_ALLOWLIST);

      AssertionUtils.assertPropertiesSubset(
            clientDefaultProps, springRemoteCacheManager.getConfiguration().properties(), "The configuration properties used by the RemoteCacheManager returned by getObject() should be equal "
                  + "to RemoteCacheManager's default settings since neither property 'configurationProperties' "
                  + "nor property 'configurationPropertiesFileLocation' has been set. However, those two are not equal."
      );
      objectUnderTest.destroy();
      defaultRemoteCacheManager.stop();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#isSingleton()}
    * .
    */
   @Test
   public final void isSingletonShouldAlwaysReturnTrue() {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();

      assertTrue(objectUnderTest.isSingleton(), "isSingleton() should always return true since each AbstractRemoteCacheManagerFactory will always produce "
            + "the same RemoteCacheManager instance. However,it returned false.");
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#destroy()}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void destroyShouldStopTheProducedCache() throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.afterPropertiesSet();
      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      objectUnderTest.destroy();

      assertFalse(remoteCacheManager.isStarted(), "destroy() should have stopped the RemoteCacheManager instance previously produced by "
            + "AbstractRemoteCacheManagerFactory. However, the produced RemoteCacheManager is still running. ");
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setConfigurationProperties(Properties)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceACacheConfiguredUsingTheSuppliedConfigurationProperties()
         throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      final Properties configurationProperties = loadConfigurationProperties(HOTROD_CLIENT_PROPERTIES_LOCATION);
      objectUnderTest.setConfigurationProperties(configurationProperties);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      AssertionUtils.assertPropertiesSubset(
            configurationProperties, remoteCacheManager.getConfiguration().properties(), "The configuration properties used by the RemoteCacheManager returned by getObject() should be equal "
                  + "to those passed into InfinispanRemoteCacheMangerFactoryBean via setConfigurationProperties(props). "
                  + "However, those two are not equal."
      );
      objectUnderTest.destroy();
   }

   private Properties loadConfigurationProperties(final Resource configurationPropertiesLocation)
         throws IOException {
      try (InputStream propsStream = HOTROD_CLIENT_PROPERTIES_LOCATION.getInputStream()) {
         final Properties configurationProperties = new Properties();
         configurationProperties.load(propsStream);

         return configurationProperties;
      }
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setConfigurationPropertiesFileLocation(Resource)}
    * .
    */
   @Test
   public final void shouldProduceACacheConfiguredUsingPropertiesLoadedFromALocationDeclaredThroughSetConfigurationPropertiesFileLocation()
         throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setConfigurationPropertiesFileLocation(HOTROD_CLIENT_PROPERTIES_LOCATION);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();
      AssertionUtils.assertPropertiesSubset(
            loadConfigurationProperties(HOTROD_CLIENT_PROPERTIES_LOCATION), remoteCacheManager.getConfiguration().properties(), "The configuration properties used by the RemoteCacheManager returned by getObject() should be equal "
                  + "to those passed into InfinispanRemoteCacheMangerFactoryBean via setConfigurationPropertiesFileLocation(propsFileLocation). "
                  + "However, those two are not equal."
      );
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setStartAutomatically(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void shouldProduceAStoppedCacheIfStartAutomaticallyIsSetToFalse() throws Exception {
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManagerExpectedToBeInStateStopped = objectUnderTest
            .getObject();

      assertFalse(remoteCacheManagerExpectedToBeInStateStopped.isStarted(),
            "AbstractRemoteCacheManagerFactory should have produced a RemoteCacheManager that is initially in state stopped "
                  + "since property 'startAutomatically' has been set to false. However, the produced RemoteCacheManager is already started.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setServerList(Collection)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setServerListShouldOverrideDefaultServerList() throws Exception {
      final Collection<InetSocketAddress> expectedServerList = new ArrayList<InetSocketAddress>(1);
      expectedServerList.add(new InetSocketAddress("testhost", 4632));
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      final String expectedServerListString = "testhost:4632";
      objectUnderTest.setServerList(expectedServerList);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(expectedServerListString, remoteCacheManager.getConfiguration().properties().get(SERVER_LIST),
            "setServerList(" + expectedServerList
                  + ") should have overridden property 'serverList'. However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setMarshaller(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setMarshallerShouldOverrideDefaultMarshaller() throws Exception {
      final String expectedMarshaller = IdentityMarshaller.class.getName();
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setMarshaller(expectedMarshaller);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(expectedMarshaller, remoteCacheManager.getConfiguration().properties().get(MARSHALLER),
            "setMarshaller(" + expectedMarshaller
                  + ") should have overridden property 'marshaller'. However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setAsyncExecutorFactory(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setAsyncExecutorFactoryShouldOverrideDefaultAsyncExecutorFactory()
         throws Exception {
      final String expectedAsyncExecutorFactory = ExecutorFactory.class.getName();
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setAsyncExecutorFactory(expectedAsyncExecutorFactory);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(expectedAsyncExecutorFactory,
            remoteCacheManager.getConfiguration().properties().get(ASYNC_EXECUTOR_FACTORY),
            "setAsyncExecutorFactory(" + expectedAsyncExecutorFactory
                  + ") should have overridden property 'asyncExecutorFactory'. However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setTcpNoDelay(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setTcpNoDelayShouldOverrideDefaultTcpNoDelay() throws Exception {
      final boolean expectedTcpNoDelay = true;
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setTcpNoDelay(expectedTcpNoDelay);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(String.valueOf(expectedTcpNoDelay),
            remoteCacheManager.getConfiguration().properties().get(TCP_NO_DELAY), "setTcpNoDelay(" + expectedTcpNoDelay
                  + ") should have overridden property 'tcpNoDelay'. However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setTcpKeepAlive(boolean)}.
    *
    * @throws Exception
    */
   @Test
   public final void setTcpKeepAliveShouldOverrideDefaultTcpKeepAlive() throws Exception {
      final boolean expectedTcpKeepAlive = false;
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setTcpNoDelay(expectedTcpKeepAlive);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(String.valueOf(expectedTcpKeepAlive),
            remoteCacheManager.getConfiguration().properties().get(TCP_KEEP_ALIVE), "setTcpKeepAlive(" + expectedTcpKeepAlive
                  + ") should have overridden property 'tcpNoDelay'. However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setRequestBalancingStrategy(String)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setRequestBalancingStrategyShouldOverrideDefaultRequestBalancingStrategy()
         throws Exception {
      final String expectedRequestBalancingStrategy = SomeRequestBalancingStrategy.class.getName();
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setRequestBalancingStrategy(expectedRequestBalancingStrategy);
      objectUnderTest.setStartAutomatically(false);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(
            expectedRequestBalancingStrategy,
            remoteCacheManager.getConfiguration().properties().get(REQUEST_BALANCING_STRATEGY),
            "setRequestBalancingStrategy("
                  + expectedRequestBalancingStrategy
                  + ") should have overridden property 'requestBalancingStrategy'. However, it didn't.");
      objectUnderTest.destroy();
   }

   /**
    * Test method for
    * {@link InfinispanRemoteCacheManagerFactoryBean#setForceReturnValues(boolean)}
    * .
    *
    * @throws Exception
    */
   @Test
   public final void setForceReturnValuesShouldOverrideDefaultForceReturnValues() throws Exception {
      final boolean expectedForceReturnValues = true;
      final InfinispanRemoteCacheManagerFactoryBean objectUnderTest = new InfinispanRemoteCacheManagerFactoryBean();
      objectUnderTest.setForceReturnValues(expectedForceReturnValues);
      objectUnderTest.afterPropertiesSet();

      final RemoteCacheManager remoteCacheManager = objectUnderTest.getObject();

      assertEquals(
            String.valueOf(expectedForceReturnValues),
            remoteCacheManager.getConfiguration().properties().get(FORCE_RETURN_VALUES),
            "setForceReturnValue(" + expectedForceReturnValues
                  + ") should have overridden property 'forceReturnValue'. However, it didn't.");
      objectUnderTest.destroy();
   }
}
