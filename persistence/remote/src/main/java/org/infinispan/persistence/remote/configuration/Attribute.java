package org.infinispan.persistence.remote.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the Remote cache store configuration
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   BALANCING_STRATEGY("balancingStrategy"),
   CONNECT_TIMEOUT("connectTimeout"),
   EXHAUSTED_ACTION("exhaustedAction"),
   FACTORY("factory"),
   FORCE_RETURN_VALUES("forceReturnValues"),
   HOST("host"),
   HOTROD_WRAPPING("hotRodWrapping"),
   MARSHALLER("marshaller"),
   MAX_ACTIVE("maxActive"),
   MAX_IDLE("maxIdle"),
   MAX_TOTAL("maxTotal"),
   MIN_EVICTABLE_IDLE_TIME("minEvictableIdleTime"),
   MIN_IDLE("minIdle"),
   KEY_SIZE_ESTIMATE("keySizeEstimate"),
   PING_ON_STARTUP("pingOnStartup"),
   PORT("port"),
   PROTOCOL_VERSION("protocolVersion"),
   RAW_VALUES("rawValues"),
   REMOTE_CACHE_NAME("remoteCacheName"),
   SOCKET_TIMEOUT("socketTimeout"),
   TCP_NO_DELAY("tcpNoDelay"),
   TEST_WHILE_IDLE("testWhileIdle"),
   TIME_BETWEEN_EVICTION_RUNS("timeBetweenEvictionRuns"),
   TRANSPORT_FACTORY("transportFactory"),
   VALUE_SIZE_ESTIMATE("valueSizeEstimate"),
   ;

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    *
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }
}
