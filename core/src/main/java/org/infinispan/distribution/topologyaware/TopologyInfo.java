package org.infinispan.distribution.topologyaware;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;


/**
 * This class holds the topology hierarchy of a cache's members.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyInfo {
   private final Map<String, Site> allSites = new HashMap<String, Site>();
   private List<Rack> allRacks = new ArrayList<Rack>();
   private List<Machine> allMachines = new ArrayList<Machine>();
   private List<Address> sortedNodes;
   private Map<Address, Float> capacityFactors;
   private Map<Address, Float> maxSegments;

   public TopologyInfo(Collection<Address> members, Map<Address, Float> capacityFactors) {
      this.capacityFactors = capacityFactors;
      this.sortedNodes = sortMembers(members, capacityFactors);

      // This way, all the nodes collections at the site/rack/machine levels will be sorted
      for (Address node : sortedNodes) {
         if (capacityFactors == null || capacityFactors.get(node) != 0.0) {
            addTopology(node);
         }
      }
   }

   private List<Address> sortMembers(Collection<Address> members, final Map<Address, Float> capacityFactors) {
      List<Address> sortedList = new ArrayList<Address>(members);
      Collections.sort(sortedList, new Comparator<Address>() {
         @Override
         public int compare(Address o1, Address o2) {
            // Sort descending by capacity factor and ascending by address (UUID)
            int capacityComparison = capacityFactors != null ? capacityFactors.get(o1).compareTo(capacityFactors.get(o2)) : 0;
            return capacityComparison != 0 ? -capacityComparison : o1.compareTo(o2);
         }
      });
      return sortedList;
   }

   private void addTopology(Address node) {
      TopologyAwareAddress taNode = (TopologyAwareAddress) node;
      String siteId = taNode.getSiteId();
      String rackId = taNode.getRackId();
      String machineId = taNode.getMachineId();

      Site site = allSites.get(siteId);
      if (site == null) {
         site = new Site(siteId);
         allSites.put(siteId, site);
      }
      Rack rack = site.racks.get(rackId);
      if (rack == null) {
         rack = new Rack(siteId, rackId);
         site.racks.put(rackId, rack);
         allRacks.add(rack);
      }
      Machine machine = rack.machines.get(machineId);
      if (machine == null) {
         machine = new Machine(siteId, rackId, machineId);
         rack.machines.put(machineId, machine);
         allMachines.add(machine);
      }
      machine.nodes.add(node);
      rack.nodes.add(node);
      site.nodes.add(node);
   }

   public Collection<Address> getSiteNodes(String site) {
      return allSites.get(site).nodes;
   }

   public Collection<Address> getRackNodes(String site, String rack) {
      return allSites.get(site).racks.get(rack).nodes;
   }

   public Collection<Address> getMachineNodes(String site, String rack, String machine) {
      return allSites.get(site).racks.get(rack).machines.get(machine).nodes;
   }

   public Set<String> getAllSites() {
      return allSites.keySet();
   }

   public Set<String> getSiteRacks(String site) {
      return allSites.get(site).racks.keySet();
   }

   public Set<String> getRackMachines(String site, String rack) {
      return allSites.get(site).racks.get(rack).machines.keySet();
   }

   public int getAllSitesCount() {
      return allSites.size();
   }

   public int getAllRacksCount() {
      return allRacks.size();
   }

   public int getAllMachinesCount() {
      return allMachines.size();
   }

   public int getAllNodesCount() {
      return sortedNodes.size();
   }

   public int getDistinctLocationsCount(TopologyLevel level, int numOwners) {
      switch (level) {
         case NODE:
            return Math.min(numOwners, getAllNodesCount());
         case MACHINE:
            return Math.min(numOwners, getAllMachinesCount());
         case RACK:
            return Math.min(numOwners, getAllRacksCount());
         case SITE:
            return Math.min(numOwners, getAllSitesCount());
         default:
            throw new IllegalArgumentException("Unexpected topology level: " + level);
      }
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("TopologyInfo{\n");
      for (Map.Entry<String, Site> site : allSites.entrySet()) {
         String siteId = site.getKey();
         sb.append(String.format("%s: {", siteId));
         for (Map.Entry<String, Rack> rack : site.getValue().racks.entrySet()) {
            String rackId = rack.getKey();
            sb.append(String.format("%s: {", rackId));
            for (Map.Entry<String, Machine> machine : rack.getValue().machines.entrySet()) {
               String machineId = machine.getKey();
               sb.append(String.format("%s: {", machineId));
               for (Address node : machine.getValue().nodes) {
                  sb.append(node);
                  sb.append(", ");
               }
               sb.setLength(sb.length() - 2);
               sb.append("}, ");
            }
            sb.setLength(sb.length() - 3);
            sb.append("}, ");
         }
         sb.setLength(sb.length() - 3);
         sb.append("}, ");
      }
      sb.setLength(sb.length() - 3);
      sb.append('}');
      return sb.toString();
   }

   /**
    * The nodes collection must be sorted in descending order by their capacity.
    */
   private double computeMaxSegmentsForNode(int numSegments, double numCopies, Collection<Address> nodes,
                                           Address node) {
      if (capacityFactors == null) {
         if (nodes.size() < numCopies) {
            return numSegments;
         } else {
            // The number of segment copies on each node should be the same
            return numCopies * numSegments / nodes.size();
         }
      }

      Float nodeCapacityFactor = capacityFactors.get(node);
      if (nodeCapacityFactor == 0)
         return 0;

      double remainingCapacity = computeTotalCapacity(nodes, capacityFactors);
      double remainingCopies = numCopies * numSegments;
      for (Address a : nodes) {
         float capacityFactor = capacityFactors.get(a);
         double nodeSegments = capacityFactor / remainingCapacity * remainingCopies;
         if (nodeSegments > numSegments) {
            nodeSegments = numSegments;
            remainingCapacity -= capacityFactor;
            remainingCopies -= nodeSegments;
            if (node.equals(a))
               return nodeSegments;
         } else {
            // All the nodes from now on will have less than numSegments segments, so we can stop the iteration
            if (!node.equals(a)) {
               nodeSegments = nodeCapacityFactor / remainingCapacity * remainingCopies;
            }
            return nodeSegments;
         }
      }
      throw new IllegalStateException("The nodes collection does not include " + node);
   }

   public float computeTotalCapacity(Collection<Address> nodes, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return nodes.size();

      float totalCapacity = 0;
      for (Address node : nodes) {
         totalCapacity += capacityFactors.get(node);
      }
      return totalCapacity;
   }

   private double computeMaxSegmentsForMachine(int numSegments, double numCopies, Collection<Machine> machines,
                                              Machine machine, Address node) {
      // The number of segment copies on each machine should be the same, except where not possible
      double copiesPerMachine = numCopies / machines.size();
      if (machine.nodes.size() <= copiesPerMachine) {
         copiesPerMachine = 1;
      } else {
         int fullMachines = 0;
         for (Machine m : machines) {
            if (m.nodes.size() <= copiesPerMachine) {
               fullMachines++;
            }
         }
         copiesPerMachine = (numCopies - fullMachines) / (machines.size() - fullMachines);
      }
      return computeMaxSegmentsForNode(numSegments, copiesPerMachine, machine.nodes, node);
   }

   private double computeMaxSegmentsForRack(int numSegments, double numCopies, Collection<Rack> racks, Rack rack,
                                           Machine machine, Address node) {
      // Not enough racks to have an owner in each rack.
      // The number of segment copies on each Rack should be the same, except where not possible
      double copiesPerRack = numCopies / racks.size();
      if (rack.machines.size() <= copiesPerRack) {
         copiesPerRack = 1;
      } else {
         int fullRacks = 0;
         for (Rack m : racks) {
            if (m.machines.size() <= copiesPerRack) {
               fullRacks++;
            }
         }
         copiesPerRack = (numCopies - fullRacks) / (racks.size() - fullRacks);
      }
      if (copiesPerRack <= 1) {
         return computeMaxSegmentsForNode(numSegments, copiesPerRack, rack.nodes, node);
      } else {
         return computeMaxSegmentsForMachine(numSegments, copiesPerRack, rack.machines.values(), machine, node);
      }
   }

   private double computeMaxSegmentsForSite(int numSegments, double numCopies, Collection<Site> sites,
                                           Site site, Rack rack, Machine machine, Address node) {
      // Not enough allSites to have an owner in each site.
      // The number of segment copies on each Site should be the same, except where not possible
      double copiesPerSite = numCopies / sites.size();
      if (site.racks.size() <= copiesPerSite) {
         copiesPerSite = 1;
      } else {
         int fullSites = 0;
         for (Site s : sites) {
            if (s.racks.size() <= copiesPerSite) {
               fullSites++;
            }
         }
         // need to compute for racks if there are enough racks in total
         copiesPerSite = (numCopies - fullSites) / (sites.size() - fullSites);
      }
      if (copiesPerSite <= 1) {
         return computeMaxSegmentsForNode(numSegments, copiesPerSite, site.nodes, node);
      } else {
         return computeMaxSegmentsForRack(numSegments, copiesPerSite, site.racks.values(), rack, machine, node);
      }
   }


   public int computeExpectedSegments(int numSegments, int numOwners, Address node) {
      if (capacityFactors != null && capacityFactors.get(node) == 0.0)
         return 0;

      TopologyAwareAddress taa = (TopologyAwareAddress) node;
      String siteId = taa.getSiteId();
      String rackId = taa.getRackId();
      String machineId = taa.getMachineId();

      Site site = allSites.get(siteId);
      Rack rack = site.racks.get(rackId);
      Machine machine = rack.machines.get(machineId);

      double maxSegments;
      if (numOwners == 1) {
         maxSegments = computeMaxSegmentsForNode(numSegments, numOwners, sortedNodes, node);
      } else if (getAllNodesCount() <= numOwners) {
         maxSegments = numSegments;
      } else if (getAllMachinesCount() <= numOwners) {
         maxSegments = computeMaxSegmentsForMachine(numSegments, numOwners, allMachines, machine, node);
      } else if (getAllRacksCount() <= numOwners) {
         maxSegments = computeMaxSegmentsForRack(numSegments, numOwners, allRacks, rack, machine, node);
      } else {
         maxSegments = computeMaxSegmentsForSite(numSegments, numOwners, allSites.values(), site, rack, machine, node);
      }
      return (int) Math.round(maxSegments);
   }
   
   private static class Site {
      String site;
      Map<String, Rack> racks = new HashMap<String, Rack>();
      List<Address> nodes = new ArrayList<Address>();

      private Site(String site) {
         this.site = site;
      }
   }

   private static class Rack {
      String site;
      String rack;
      Map<String, Machine> machines = new HashMap<String, Machine>();
      List<Address> nodes = new ArrayList<Address>();

      private Rack(String site, String rack) {
         this.site = site;
         this.rack = rack;
      }
   }

   private static class Machine {
      String site;
      String rack;
      String machine;
      List<Address> nodes = new ArrayList<Address>();

      private Machine(String site, String rack, String machine) {
         this.site = site;
         this.rack = rack;
         this.machine = machine;
      }
   }
}
