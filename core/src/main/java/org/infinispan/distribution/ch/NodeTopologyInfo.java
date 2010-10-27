package org.infinispan.distribution.ch;

/**
 * Holds topology information about a a node.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class NodeTopologyInfo {

   private final String machineId;
   private final String rackId;
   private final String siteId;

   public NodeTopologyInfo(String machineId, String rackId, String siteId) {
      this.machineId = machineId;
      this.rackId = rackId;
      this.siteId = siteId;
   }

   public String getMachineId() {
      return machineId;
   }

   public String getRackId() {
      return rackId;
   }

   public String getSiteId() {
      return siteId;
   }

   public boolean sameSite(NodeTopologyInfo info2) {
      return equalObjects(siteId, info2.siteId);
   }

   public boolean sameRack(NodeTopologyInfo info2) {
      return sameSite(info2) && equalObjects(rackId, info2.rackId);
   }

   public boolean sameMachine(NodeTopologyInfo info2) {
      return sameRack(info2) && equalObjects(machineId, info2.machineId);
   }

   private boolean equalObjects(Object first, Object second) {
      return first == null ? second == null : first.equals(second);
   }
}
