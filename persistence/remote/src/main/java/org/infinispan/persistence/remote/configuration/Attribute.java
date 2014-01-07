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

   ASYNC_EXECUTOR("async-executor"),
   BALANCING_STRATEGY("balancing-strategy"),
   CONNECT_TIMEOUT("connect-timeout"),
   EXHAUSTED_ACTION("exhausted-action"),
   FACTORY("factory"),
   FORCE_RETURN_VALUES("force-return-values"),
   HOST("host"),
   HOTROD_WRAPPING("hotrod-wrapping"),
   MARSHALLER("marshaller"),
   MAX_ACTIVE("max-active"),
   MAX_IDLE("max-idle"),
   MAX_TOTAL("max-total"),
   MIN_EVICTABLE_IDLE_TIME("min-evictable-idle-time"),
   MIN_IDLE("min-idle-time"),
   KEY_SIZE_ESTIMATE("key-size-estimate"),
   OUTBOUND_SOCKET_BINDING("outbound-socket-binding"),
   PING_ON_STARTUP("ping-on-start"),
   PORT("port"),
   PROTOCOL_VERSION("protocol-version"),
   RAW_VALUES("raw-values"),
   REMOTE_CACHE_NAME("cache"),
   SOCKET_TIMEOUT("socket-timeout"),
   TCP_NO_DELAY("tcp-no-delay"),
   TEST_WHILE_IDLE("test-idle"),
   TIME_BETWEEN_EVICTION_RUNS("eviction-interval"),
   TRANSPORT_FACTORY("transport-factory"),
   VALUE_SIZE_ESTIMATE("value-size-estimate"),
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
