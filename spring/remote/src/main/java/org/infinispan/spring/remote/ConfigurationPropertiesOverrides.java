package org.infinispan.spring.remote;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.JAVA_SERIAL_ALLOWLIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * <p>
 * Provides a mechanism to override selected configuration properties using explicit setters for
 * each configuration setting.
 * </p>
 *
 * @author Olaf Bergner
 *
 */
public class ConfigurationPropertiesOverrides {
   public static final String OPERATION_READ_TIMEOUT = "infinispan.spring.operation.read.timeout";
   public static final String OPERATION_WRITE_TIMEOUT = "infinispan.spring.operation.write.timeout";

   private final Properties overridingProperties = new Properties();

   /**
    * @return
    * @see java.util.Hashtable#isEmpty()
    */
   public boolean isEmpty() {
      return this.overridingProperties.isEmpty();
   }

   boolean containsProperty(String key) {
      String prop = overridingProperties.getProperty(key);
      return prop != null && !prop.isEmpty();
   }

   /**
    * @param serverList
    */
   public void setServerList(final Collection<InetSocketAddress> serverList) {
      final StringBuilder serverListStr = new StringBuilder();
      for (final InetSocketAddress server : serverList) {
         serverListStr.append(server.getHostString()).append(":").append(server.getPort())
               .append(";");
      }
      serverListStr.deleteCharAt(serverListStr.length() - 1);
      this.overridingProperties.setProperty(SERVER_LIST, serverListStr.toString());
   }

   /**
    * @param marshaller
    */
   public void setMarshaller(final String marshaller) {
      this.overridingProperties.setProperty(MARSHALLER, marshaller);
   }

   /**
    * @param allowListRegex
    */
   public void setClassAllowList(final String allowListRegex) {
      this.overridingProperties.setProperty(JAVA_SERIAL_ALLOWLIST, allowListRegex);
   }

   /**
    * @param asyncExecutorFactory
    */
   public void setAsyncExecutorFactory(final String asyncExecutorFactory) {
      this.overridingProperties.setProperty(ASYNC_EXECUTOR_FACTORY, asyncExecutorFactory);
   }

   /**
    * @param tcpNoDelay
    */
   public void setTcpNoDelay(final boolean tcpNoDelay) {
      this.overridingProperties.setProperty(TCP_NO_DELAY, Boolean.toString(tcpNoDelay));
   }

   public void setTcpKeepAlive(final boolean tcpKeepAlive) {
      this.overridingProperties.setProperty(TCP_KEEP_ALIVE, Boolean.toString(tcpKeepAlive));
   }

   /**
    * @param requestBalancingStrategy
    */
   public void setRequestBalancingStrategy(final String requestBalancingStrategy) {
      this.overridingProperties.setProperty(REQUEST_BALANCING_STRATEGY, requestBalancingStrategy);
   }

   /**
    * @param forceReturnValues
    */
   public void setForceReturnValues(final boolean forceReturnValues) {
      this.overridingProperties.setProperty(FORCE_RETURN_VALUES,
                                            Boolean.toString(forceReturnValues));
   }

   public void setReadTimeout(long readTimeout) {
      this.overridingProperties.setProperty(OPERATION_READ_TIMEOUT, Long.toString(readTimeout));
   }

   public void setWriteTimeout(long writeTimeout) {
      this.overridingProperties.setProperty(OPERATION_WRITE_TIMEOUT, Long.toString(writeTimeout));
   }

   /**
    * @param configurationPropertiesToOverride
    * @return
    */
   public Properties override(final Properties configurationPropertiesToOverride) {
      final Properties answer = Properties.class.cast(configurationPropertiesToOverride.clone());
      for (final Map.Entry<Object, Object> prop : this.overridingProperties.entrySet()) {
         answer.setProperty(String.class.cast(prop.getKey()), String.class.cast(prop.getValue()));
      }
      return answer;
   }
}
