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
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public enum ClientIntelligence {
   BASIC(1),
   TOPOLOGY_AWARE(2),
   HASH_DISTRIBUTION_AWARE(3);

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
}
