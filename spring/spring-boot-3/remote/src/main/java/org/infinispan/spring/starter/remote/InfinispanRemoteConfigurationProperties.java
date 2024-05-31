package org.infinispan.spring.starter.remote;

import java.util.Map;
import java.util.Properties;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(value = "infinispan.remote", ignoreInvalidFields = true)
public class InfinispanRemoteConfigurationProperties extends org.infinispan.client.hotrod.impl.ConfigurationProperties {
   public static final String DEFAULT_CLIENT_PROPERTIES = "classpath:hotrod-client.properties";

   /**
    * Enable remote cache.
    */
   private boolean enabled = true;

   /**
    * The hotrod client properties location.
    */
   private String clientProperties = DEFAULT_CLIENT_PROPERTIES;

   private boolean reactive = false;
   private long readTimeout = 0;
   private long writeTimeout = 0;

   public String getClientProperties() {
      return clientProperties;
   }

   public void setClientProperties(String clientProperties) {
      this.clientProperties = clientProperties;
   }

   public boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(boolean enabled) {
      this.enabled = enabled;
   }


   public ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      Properties properties = this.getProperties();
      builder.withProperties(properties);
      return builder;
   }

   public void setSaslProperties(Map<String, String> saslProperties) {
      saslProperties.forEach((k, v) -> this.getProperties().setProperty(SASL_PROPERTIES_PREFIX + "." + k, v));
   }

   public void setCluster(Map<String, String> cluster) {
      cluster.forEach((k, v) -> this.getProperties().setProperty(CLUSTER_PROPERTIES_PREFIX + "." + k, v));
   }

   public boolean isReactive() {
      return reactive;
   }

   public void setReactive(boolean reactive) {
      this.reactive = reactive;
   }

   public long getReadTimeout() {
      return readTimeout;
   }

   public void setReadTimeout(long readTimeout) {
      this.readTimeout = readTimeout;
   }

   public long getWriteTimeout() {
      return writeTimeout;
   }

   public void setWriteTimeout(long writeTimeout) {
      this.writeTimeout = writeTimeout;
   }
}
