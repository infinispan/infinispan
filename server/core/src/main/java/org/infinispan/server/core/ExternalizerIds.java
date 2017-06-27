package org.infinispan.server.core;

/**
 * Externalizer ids used by Server module {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementations.
 * TODO: update URL below
 * Information about the valid id range can be found <a href="http://community.jboss.org/docs/DOC-16198">here</a>
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 9.0
 */
public final class ExternalizerIds {
   private ExternalizerIds() { }

   public static final int SERVER_ENTRY_VERSION = 1100;
   public static final int MEMCACHED_METADATA = 1101;
   public static final int TOPOLOGY_ADDRESS = 1102;
   public static final int TOPOLOGY_VIEW = 1103;
   public static final int SERVER_ADDRESS = 1104;
   public static final int MIME_METADATA = 1105;
   public static final int BINARY_FILTER = 1106;
   public static final int BINARY_CONVERTER = 1107;
   public static final int KEY_VALUE_VERSION_CONVERTER = 1108;
   public static final int BINARY_FILTER_CONVERTER = 1109;
   public static final int KEY_VALUE_WITH_PREVIOUS_CONVERTER = 1110;
   public static final int ITERATION_FILTER = 1111;
   public static final int QUERY_ITERATION_FILTER = 1112;
   public static final int TX_STATE = 1113;
   public static final int CACHE_XID = 1114;
   public static final int TX_FUNCTIONS = 1115;

}
