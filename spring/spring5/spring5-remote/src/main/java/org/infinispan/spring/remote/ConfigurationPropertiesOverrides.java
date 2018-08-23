package org.infinispan.spring.remote;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.ASYNC_EXECUTOR_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.FORCE_RETURN_VALUES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.KEY_SIZE_ESTIMATE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.MARSHALLER;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.NEAR_CACHE_MAX_ENTRIES;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.NEAR_CACHE_MODE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.NEAR_CACHE_NAME_PATTERN;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.REQUEST_BALANCING_STRATEGY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.SERVER_LIST;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_KEEP_ALIVE;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TCP_NO_DELAY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.TRANSPORT_FACTORY;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.VALUE_SIZE_ESTIMATE;

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
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
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

   /**
    * @param TransportFactory
    */
   public void setTransportFactory(final String TransportFactory) {
      this.overridingProperties.setProperty(TRANSPORT_FACTORY, TransportFactory);
   }

   /**
    * @param serverList
    */
   public void setServerList(final Collection<InetSocketAddress> serverList) {
      final StringBuilder serverListStr = new StringBuilder();
      for (final InetSocketAddress server : serverList) {
         serverListStr.append(server.getHostName()).append(":").append(server.getPort())
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
    * @param keySizeEstimate
    */
   public void setKeySizeEstimate(final int keySizeEstimate) {
      this.overridingProperties.setProperty(KEY_SIZE_ESTIMATE, Integer.toString(keySizeEstimate));
   }

   /**
    * @param valueSizeEstimate
    */
   public void setValueSizeEstimate(final int valueSizeEstimate) {
      this.overridingProperties.setProperty(VALUE_SIZE_ESTIMATE,
                                            Integer.toString(valueSizeEstimate));
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

   public void setNearCacheMode(String mode) {
      this.overridingProperties.setProperty(NEAR_CACHE_MODE, mode);
   }

   public void setNearCacheMaxEntries(int maxEntries) {
      this.overridingProperties.setProperty(NEAR_CACHE_MAX_ENTRIES, Integer.toString(maxEntries));
   }

   public void setNearCacheNamePattern(String pattern) {
      this.overridingProperties.setProperty(NEAR_CACHE_NAME_PATTERN, pattern);
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
