package org.infinispan.client.hotrod.configuration;

/**
 * ClientIntelligence specifies the level of intelligence used by the client.
 * <ul> <li><b>BASIC</b> means that the
 * client doesn't handle server topology changes and therefore will only used the list of servers supplied at
 * configuration time</li>
 * <li><b>TOPOLOGY_AWARE</b> means that the client wants to receive topology updates from the
 * servers so that it can deal with added / removed servers dynamically. Requests will go to the servers using a
 * round-robin approach</li>
 * <li><b>HASH_DISTRIBUTION_AWARE</b> like <i>TOPOLOGY_AWARE</i> but with the additional
 * advantage that each request involving keys will be routed to the server who is the primary owner which improves
 * performance greatly. This is the default</li>
 * <li><b>AUTO</b> starts as <i>HASH_DISTRIBUTION_AWARE</i> and automatically degrades to <i>BASIC</i> if connecting
 * to the server-provided topology is impossible. This is useful in environments such as containers running on
 * non-Linux platforms or when NAT is involved</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public enum ClientIntelligence {
   BASIC(1),
   TOPOLOGY_AWARE(2),
   HASH_DISTRIBUTION_AWARE(3),
   AUTO(4);

   final byte value;

   ClientIntelligence(int value) {
      this.value = (byte) value;
   }

   public byte getValue() {
      return value;
   }

   public static ClientIntelligence getDefault() {
      return HASH_DISTRIBUTION_AWARE;
   }

   /**
    * Returns the effective intelligence level to use for operations.
    * For AUTO mode, this returns HASH_DISTRIBUTION_AWARE as the initial level.
    *
    * @return the effective client intelligence
    */
   public ClientIntelligence getEffectiveIntelligence() {
      return this == AUTO ? HASH_DISTRIBUTION_AWARE : this;
   }

   /**
    * Returns true if this intelligence level supports topology updates.
    *
    * @return true if topology-aware, false otherwise
    */
   public boolean isTopologyAware() {
      return this == TOPOLOGY_AWARE || this == HASH_DISTRIBUTION_AWARE || this == AUTO;
   }
}
