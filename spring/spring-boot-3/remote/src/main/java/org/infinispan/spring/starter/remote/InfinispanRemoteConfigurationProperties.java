package org.infinispan.spring.starter.remote;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;


@ConfigurationProperties(value = "infinispan.remote", ignoreInvalidFields = true)
public class InfinispanRemoteConfigurationProperties extends org.infinispan.client.hotrod.impl.ConfigurationProperties {
   public static final String DEFAULT_CLIENT_PROPERTIES = "classpath:hotrod-client.properties";
   private static final Pattern WILDCARD_PATTERN = Pattern.compile(".*\\*.*");
   private static final Pattern ORG_INFINISPAN_PATTERN = Pattern.compile("org\\.infinispan\\..*");

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

   public void setCache(Map<String, Map<String, Object>> cachesProperties) {
      cachesProperties.forEach(this::processCacheEntry);
   }

   private void processCacheEntry(String cacheKey, Map<String, Object> cacheValue) {
      // Determine the proper cache key format for Infinispan
      String infinispanCacheKey = formatCacheKeyForInfinispan(cacheKey);

      // Flatten and set all properties for this cache
      flattenAndSetProperties(infinispanCacheKey, cacheValue);
   }
   private String formatCacheKeyForInfinispan(String cacheKey) {
      // Already has brackets - use as is
      if (cacheKey.startsWith("[") && cacheKey.endsWith("]")) {
         return cacheKey;
      }

      // Needs brackets for patterns or org.infinispan caches
      if (WILDCARD_PATTERN.matcher(cacheKey).matches() ||
              ORG_INFINISPAN_PATTERN.matcher(cacheKey).matches()) {
         return "[" + cacheKey + "]";
      }

      // Regular cache name
      return cacheKey;
   }

   private void flattenAndSetProperties(String cacheKey, Map<String, Object> properties) {
      flattenMap(properties, "").forEach((key, value) -> {
         String propertyKey = CACHE_PREFIX + cacheKey + "." + key.replace("-", "_");
         this.getProperties().setProperty(propertyKey, value.toString());
      });
   }

   private Map<String, Object> flattenMap(Map<String, Object> source, String prefix) {
      Map<String, Object> result = new HashMap<>();

      source.forEach((key, value) -> {
         String newKey = prefix.isEmpty() ? key : prefix + "." + key;

         if (value instanceof Map) {
            Map<String, Object> nestedMap = (Map<String, Object>) value;
            result.putAll(flattenMap(nestedMap, newKey));
         } else {
            result.put(newKey, value);
         }
      });

      return result;
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
