package org.infinispan.health;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * Cluster health information.
 */
public interface ClusterHealth extends JsonSerialization {

   /**
    * Returns total cluster health.
    */
   HealthStatus getHealthStatus();

   /**
    * Returns the name of the cluster.
    */
   String getClusterName();

   /**
    * Returns the number of nodes in the cluster.
    */
   int getNumberOfNodes();

   /**
    * Returns node names.
    */
   List<String> getNodeNames();
}
