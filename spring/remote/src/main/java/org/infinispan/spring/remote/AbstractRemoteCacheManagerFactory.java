package org.infinispan.spring.remote;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.Util;
import org.springframework.core.io.Resource;

/**
 * <p>
 * An abstract base class for factories creating cache manager that are backed by an Infinispan
 * RemoteCacheManager.
 * </p>
 *
 * @author Olaf Bergner
 *
 * @see RemoteCacheManager
 */
public abstract class AbstractRemoteCacheManagerFactory {

   protected static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   public static final String SPRING_JAVA_SERIAL_ALLOWLIST = "java.util.HashMap,java.time.*,org.springframework.*,org.infinispan.spring.remote.*";

   protected boolean startAutomatically = true;

   private Properties configurationProperties;

   private Resource configurationPropertiesFileLocation;

   private final ConfigurationPropertiesOverrides configurationPropertiesOverrides = new ConfigurationPropertiesOverrides();

   protected void assertCorrectlyConfigured() throws IllegalStateException {
      if ((this.configurationProperties != null)
            && (this.configurationPropertiesFileLocation != null)) {
         throw new IllegalStateException(
               "You may only use either \"configurationProperties\" or \"configurationPropertiesFileLocation\" "
                     + "to configure the RemoteCacheManager, not both.");
      } else if ((this.configurationProperties != null)
            && !this.configurationPropertiesOverrides.isEmpty()) {
         throw new IllegalStateException(
               "You may only use either \"configurationProperties\" or setters on this FactoryBean "
                     + "to configure the RemoteCacheManager, not both.");
      }
   }

   protected Properties configurationProperties() throws IOException {
      final Properties answer;
      if (this.configurationProperties != null) {
         answer = this.configurationPropertiesOverrides.override(this.configurationProperties);
         logger.debug("Using user-defined properties [" + this.configurationProperties
                                 + "] for configuring RemoteCacheManager");
      } else if (this.configurationPropertiesFileLocation != null) {
         answer = loadPropertiesFromFile(this.configurationPropertiesFileLocation);
         logger.debug("Loading properties from file [" + this.configurationProperties
                                 + "] for configuring RemoteCacheManager");
      } else if (!this.configurationPropertiesOverrides.isEmpty()) {
         answer = this.configurationPropertiesOverrides.override(new Properties());
         logger.debug("Using explicitly set configuration settings [" + answer
                                 + "] for configuring RemoteCacheManager");
      } else {
         logger.debug("No configuration properties. RemoteCacheManager will use default configuration.");
         RemoteCacheManager remoteCacheManager = new RemoteCacheManager(false);
         try {
            answer = remoteCacheManager.getConfiguration().properties();
         } finally {
           remoteCacheManager.stop();
         }
      }
      initializeMarshallingProperties(answer);
      return answer;
   }

   private void initializeMarshallingProperties(Properties properties) {
      // If the property has already been overridden, we don't apply any default behaviour
      if (!configurationPropertiesOverrides.containsProperty(ConfigurationProperties.MARSHALLER)) {
         String marshaller = JavaSerializationMarshaller.class.getName();
         String configuredMarshaller = properties.getProperty(ConfigurationProperties.MARSHALLER);
         if (configuredMarshaller != null && configuredMarshaller.equals(Util.GENERIC_JBOSS_MARSHALLING_CLASS)) {
            // If the marshaller is equal to the legacy jboss-marshaller, and infinispan-jboss-marshalling is on the cp, allow
            if (Util.getJBossMarshaller(Thread.currentThread().getContextClassLoader(), null) != null)
               marshaller = configuredMarshaller;
         }
         properties.setProperty(ConfigurationProperties.MARSHALLER, marshaller);
      }

      if (!configurationPropertiesOverrides.containsProperty(ConfigurationProperties.JAVA_SERIAL_ALLOWLIST)) {
         Set<String> userRegexSet = new LinkedHashSet<>();
         Collections.addAll(userRegexSet, SPRING_JAVA_SERIAL_ALLOWLIST.split(","));
         String userAllowList = properties.getProperty(ConfigurationProperties.JAVA_SERIAL_ALLOWLIST);
         if (userAllowList != null && !userAllowList.isEmpty()) {
            Collections.addAll(userRegexSet, userAllowList.split(","));
         }
         String allowList = String.join(",", userRegexSet);
         properties.setProperty(ConfigurationProperties.JAVA_SERIAL_ALLOWLIST, allowList);
      }
   }

   private Properties loadPropertiesFromFile(final Resource propertiesFileLocation)
         throws IOException {
      try (InputStream propsStream = propertiesFileLocation.getInputStream()) {
         final Properties answer = new Properties();
         answer.load(propsStream);
         return answer;
      }
   }

   // ------------------------------------------------------------------------
   // Setters for configuring RemoteCacheManager
   // ------------------------------------------------------------------------

   /**
    * @param configurationProperties
    *           the configurationProperties to set
    */
   public void setConfigurationProperties(final Properties configurationProperties) {
      this.configurationProperties = configurationProperties;
   }

   /**
    * @param configurationPropertiesFileLocation
    *           the configurationPropertiesFileLocation to set
    */
   public void setConfigurationPropertiesFileLocation(
         final Resource configurationPropertiesFileLocation) {
      this.configurationPropertiesFileLocation = configurationPropertiesFileLocation;
   }

   /**
    * @param startAutomatically
    *           the startAutomatically to set
    */
   public void setStartAutomatically(final boolean startAutomatically) {
      this.startAutomatically = startAutomatically;
   }

   /**
    * @param serverList
    * @see ConfigurationPropertiesOverrides#setServerList(Collection)
    */
   public void setServerList(final Collection<InetSocketAddress> serverList) {
      this.configurationPropertiesOverrides.setServerList(serverList);
   }

   /**
    * @param marshaller
    * @see ConfigurationPropertiesOverrides#setMarshaller(String)
    */
   public void setMarshaller(final String marshaller) {
      this.configurationPropertiesOverrides.setMarshaller(marshaller);
   }

   /**
    * @param allowListRegex
    * @see ConfigurationPropertiesOverrides#setClassAllowList(String)
    */
   public void setClassAllowList(final String allowListRegex) {
      this.configurationPropertiesOverrides.setClassAllowList(allowListRegex);
   }

   /**
    * @param asyncExecutorFactory
    * @see ConfigurationPropertiesOverrides#setAsyncExecutorFactory(String)
    */
   public void setAsyncExecutorFactory(final String asyncExecutorFactory) {
      this.configurationPropertiesOverrides.setAsyncExecutorFactory(asyncExecutorFactory);
   }

   /**
    * @param tcpNoDelay
    * @see ConfigurationPropertiesOverrides#setTcpNoDelay(boolean)
    */
   public void setTcpNoDelay(final boolean tcpNoDelay) {
      this.configurationPropertiesOverrides.setTcpNoDelay(tcpNoDelay);
   }

   /**
    * @see ConfigurationPropertiesOverrides#setTcpNoDelay(boolean)
    */
   public void setTcpKeepAlive(final boolean tcpKeepAlive) {
      this.configurationPropertiesOverrides.setTcpKeepAlive(tcpKeepAlive);
   }

   /**
    * @param requestBalancingStrategy
    * @see ConfigurationPropertiesOverrides#setRequestBalancingStrategy(String)
    */
   public void setRequestBalancingStrategy(final String requestBalancingStrategy) {
      this.configurationPropertiesOverrides.setRequestBalancingStrategy(requestBalancingStrategy);
   }

   /**
    * @param forceReturnValues
    * @see ConfigurationPropertiesOverrides#setForceReturnValues(boolean)
    */
   public void setForceReturnValues(final boolean forceReturnValues) {
      this.configurationPropertiesOverrides.setForceReturnValues(forceReturnValues);
   }

   /**
    * @param readTimeout
    * @see ConfigurationPropertiesOverrides#setReadTimeout(long)
    */
   public void setReadTimeout(final long readTimeout) {
      this.configurationPropertiesOverrides.setReadTimeout(readTimeout);
   }

   /**
    * @param writeTimeout
    * @see ConfigurationPropertiesOverrides#setWriteTimeout(long)
    */
   public void setWriteTimeout(final long writeTimeout) {
      this.configurationPropertiesOverrides.setWriteTimeout(writeTimeout);
   }
}
