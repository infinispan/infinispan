package org.infinispan.client.hotrod.impl;

import java.util.Objects;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;

/**
 * Keeps the Hot Rod client topology information together: topology id and client intelligence.
 *
 * @since 14.0
 */
public final class ClientTopology {

   private final int topologyId;
   private final ClientIntelligence clientIntelligence;

   public ClientTopology(int topologyId, ClientIntelligence clientIntelligence) {
      this.topologyId = topologyId;
      this.clientIntelligence = Objects.requireNonNull(clientIntelligence);
   }

   public int getTopologyId() {
      return topologyId;
   }

   public ClientIntelligence getClientIntelligence() {
      return clientIntelligence;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClientTopology that = (ClientTopology) o;

      return topologyId == that.topologyId && clientIntelligence == that.clientIntelligence;
   }

   @Override
   public int hashCode() {
      int result = topologyId;
      result = 31 * result + clientIntelligence.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "ClientTopology{" +
            "topologyId=" + topologyId +
            ", clientIntelligence=" + clientIntelligence +
            '}';
   }
}
