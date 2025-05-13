package org.infinispan.distribution.topologyaware;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;


/**
 * This class holds the topology hierarchy of a cache's members and estimates for owned segments.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyInfo {
   private final int numSegments;
   private final int numOwners;
   private final Cluster cluster = new Cluster();
   private final List<Rack> allRacks = new ArrayList<>();
   private final List<Machine> allMachines = new ArrayList<>();
   private final List<Node> allNodes = new ArrayList<>();
   private final Map<Address, Node> addressMap = new HashMap<>();

   public TopologyInfo(int numSegments, int numOwners, Collection<Address> members,
                       Map<Address, Float> capacityFactors) {
      this.numOwners = Math.min(numOwners, members.size());
      this.numSegments = numSegments;

      // This way, all the nodes collections at the id/rack/machine levels will be sorted
      for (Address node : members) {
         float capacityFactor = capacityFactors != null ? capacityFactors.get(node) : 1f;
         if (capacityFactor != 0f) {
            addNode(node, capacityFactor);
         }
      }

      if (cluster.totalCapacity == 0)
         throw new IllegalArgumentException("At least one node should have non-zero capacity");

      // Sort all location lists descending by capacity factor and add them to the global collections
      // splitExpectedOwnedSegments needs location lists to be sorted
      Collections.sort(cluster.sites);
      for (Site site : cluster.sites) {
         Collections.sort(site.racks);
         for (Rack rack : site.racks) {
            allRacks.add(rack);
            Collections.sort(rack.machines);
            for (Machine machine : rack.machines) {
               allMachines.add(machine);
               Collections.sort(machine.nodes);
               for (Node node : machine.nodes) {
                  allNodes.add(node);
                  addressMap.put(node.address, node);
               }
            }
         }
      }

      computeExpectedSegments();
   }

   public int getDistinctLocationsCount(TopologyLevel level) {
      switch (level) {
         case NODE:
            return allNodes.size();
         case MACHINE:
            return allMachines.size();
         case RACK:
            return allRacks.size();
         case SITE:
            return cluster.sites.size();
         default:
            throw new IllegalArgumentException("Unknown level: " + level);
      }
   }

   public int getDistinctLocationsCount(TopologyLevel level, Collection<Address> addresses) {
      Set<Object> locations = new HashSet<>();
      for (Address address : addresses) {
         locations.add(getLocationId(level, address));
      }
      return locations.size();
   }

   public boolean duplicateLocation(TopologyLevel level, Collection<Address> addresses, Address candidate, boolean excludeCandidate) {
      Object newLocationId = getLocationId(level, candidate);
      for (Address address : addresses) {
         if (!excludeCandidate || !address.equals(candidate)) {
            if (newLocationId.equals(getLocationId(level, address)))
               return true;
         }
      }
      return false;
   }

   public Object getLocationId(TopologyLevel level, Address address) {
      Node node = addressMap.get(address);
      Object locationId;
      switch (level) {
         case SITE:
            locationId = node.machine.rack.site;
            break;
         case RACK:
            locationId = node.machine.rack;
            break;
         case MACHINE:
            locationId = node.machine;
            break;
         case NODE:
            locationId = node;
            break;
         default:
            throw new IllegalStateException("Unexpected value: " + level);
      }
      return locationId;
   }

   private void addNode(Address address, float capacityFactor) {
      TopologyAwareAddress taa = (TopologyAwareAddress) address;
      String siteId = taa.getSiteId();
      String rackId = taa.getRackId();
      String machineId = taa.getMachineId();

      cluster.addNode(siteId, rackId, machineId, address, capacityFactor);
   }

   public Collection<Address> getSiteNodes(String site) {
      Collection<Address> addresses = new ArrayList<>();
      cluster.getSite(site).collectNodes(addresses);
      return addresses;
   }

   public Collection<Address> getRackNodes(String site, String rack) {
      Collection<Address> addresses = new ArrayList<>();
      cluster.getSite(site).getRack(rack).collectNodes(addresses);
      return addresses;
   }

   public Collection<Address> getMachineNodes(String site, String rack, String machine) {
      Collection<Address> addresses = new ArrayList<>();
      cluster.getSite(site).getRack(rack).getMachine(machine).collectNodes(addresses);
      return addresses;
   }

   public Collection<String> getAllSites() {
      return cluster.getChildNames();
   }

   public Collection<String> getSiteRacks(String site) {
      return cluster.getSite(site).getChildNames();
   }

   public Collection<String> getRackMachines(String site, String rack) {
      return cluster.getSite(site).getRack(rack).getChildNames();
   }

   @Override
   public String toString() {
      DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
      df.setMaximumFractionDigits(2);
      StringBuilder sb = new StringBuilder("TopologyInfo{\n");
      sb.append(formatLocation(df, cluster, ""));
      for (Site site : cluster.sites) {
         sb.append(formatLocation(df, site, "  "));
         for (Rack rack : site.racks) {
            sb.append(formatLocation(df, rack, "    "));
            for (Machine machine : rack.machines) {
               sb.append(formatLocation(df, machine, "      "));
               for (Node node : machine.nodes) {
                  sb.append(formatLocation(df, node, "        "));
               }
            }
         }
      }
      sb.append("}");
      return sb.toString();
   }

   public String formatLocation(DecimalFormat df, Location location, String prefix) {
      return String.format("%s%s * %s: %s+%s %n", prefix, location.getName(), df.format(location.totalCapacity),
                           df.format(location.expectedPrimarySegments),
                           df.format(location.getExpectedBackupSegments()));
   }

   private void computeExpectedSegments() {
      // Primary owners are allocated strictly based on capacity factors
      // But for backup owners we first try to put an owner on each site, on every rack, and on every machine
      // If there are too few nodes, each node will hold all the segments
      // If there are too few machines, each machine will hold all the segments, and some will have duplicates
      // The same if there are too few racks or sites

      splitPrimarySegments();

      splitExpectedOwnedSegments(cluster.getChildren(), numSegments * numOwners,
                                 cluster.totalCapacity);
   }

   private void splitPrimarySegments() {
      // Round down, the actual segment allocation is allowed to add extra segments
      for (Node node : allNodes) {
         float fraction = node.totalCapacity / cluster.totalCapacity;
         node.addPrimarySegments(numSegments * fraction);
      }
   }

   /**
    * Split totalOwnedSegments segments into the given locations recursively.
    *
    * @param locations List of locations of the same level, sorted descending by capacity factor
    */
   private void splitExpectedOwnedSegments(Collection<? extends Location> locations, float totalOwnedSegments,
                                           float totalCapacity) {
      float remainingCapacity = totalCapacity;
      float remainingOwned = totalOwnedSegments;

      // First pass, assign expected owned segments for locations with too little capacity
      // We know we can do it without a loop because locations are ordered descending by capacity
      List<Location> remainingLocations = new ArrayList<>(locations);
      for (ListIterator<Location> it = remainingLocations.listIterator(locations.size()); it.hasPrevious(); ) {
         Location location = it.previous();

         if (remainingOwned < numSegments * remainingLocations.size())
            break;

         // We don't have enough locations, so each location must own at least numSegments segments
         int minOwned = numSegments;
         float locationOwned = remainingOwned * location.totalCapacity / remainingCapacity;
         if (locationOwned > minOwned)
            break;

         splitExpectedOwnedSegments2(location.getChildren(), minOwned, location.totalCapacity);
         remainingCapacity -= location.totalCapacity;
         remainingOwned -= location.expectedOwnedSegments;
         it.remove();
      }

      // Second pass, assign expected owned segments for locations with too much capacity
      // We know we can do it without a loop because locations are ordered descending by capacity
      for (Iterator<? extends Location> it = remainingLocations.iterator(); it.hasNext(); ) {
         Location location = it.next();

         float maxOwned = computeMaxOwned(remainingOwned, remainingLocations.size());

         float locationOwned = remainingOwned * location.totalCapacity / remainingCapacity;
         if (locationOwned < maxOwned)
            break;

         splitExpectedOwnedSegments2(location.getChildren(), maxOwned, location.totalCapacity);
         remainingCapacity -= location.totalCapacity;
         remainingOwned -= maxOwned;
         it.remove();
      }

      // If there were exactly numSegments segments per location, we're finished here
      if (remainingLocations.isEmpty())
         return;

      // Third pass: If more than numSegments segments per location, split segments between their children
      // Else spread remaining segments based only on the capacity, rounding down
      if (remainingLocations.size() * numSegments < remainingOwned) {
         List<Location> childrenLocations = new ArrayList<>(remainingLocations.size() * 2);
         for (Location location : remainingLocations) {
            childrenLocations.addAll(location.getChildren());
         }
         Collections.sort(childrenLocations);
         splitExpectedOwnedSegments2(childrenLocations, remainingOwned, remainingCapacity);
      } else {
         // The allocation algorithm can assign more segments to nodes, so it's ok to miss some segments here
         float fraction = remainingOwned / remainingCapacity;
         for (Location location : remainingLocations) {
            float locationOwned = location.totalCapacity * fraction;
            splitExpectedOwnedSegments2(location.getChildren(), locationOwned, location.totalCapacity);
         }
      }
   }

   private float computeMaxOwned(float remainingOwned, int locationsCount) {
      float maxOwned;
      if (remainingOwned < numSegments) {
         // We already have enough owners on siblings of the parent location
         maxOwned = remainingOwned;
      } else if (remainingOwned < numSegments * locationsCount) {
         // We have enough locations to so we don't put more than numSegments segments in any of them
         maxOwned = numSegments;
      } else {
         // We don't have enough locations, so each location gets at least numSegments
         maxOwned = remainingOwned - numSegments * (locationsCount - 1);
      }
      return maxOwned;
   }

   private void splitExpectedOwnedSegments2(Collection<? extends Location> locations,
                                            float totalOwnedSegments, float totalCapacity) {
      Location first = locations.iterator().next();
      if (locations.size() == 1 && first instanceof Node) {
         ((Node) first).addOwnedSegments(totalOwnedSegments);
      } else {
         splitExpectedOwnedSegments(locations, totalOwnedSegments, totalCapacity);
      }
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

   public float getExpectedPrimarySegments(Address address) {
      Node node = addressMap.get(address);
      return node != null ? node.expectedPrimarySegments : 0;
   }

   public float getExpectedOwnedSegments(Address address) {
      Node node = addressMap.get(address);
      return node != null ? node.expectedOwnedSegments : 0;
   }

   public int getSiteIndex(Address address) {
      Site site = addressMap.get(address).getSite();
      return cluster.sites.indexOf(site);
   }

   public int getRackIndex(Address address) {
      Rack rack = addressMap.get(address).getRack();
      return allRacks.indexOf(rack);
   }

   public int getMachineIndex(Address address) {
      Machine machine = addressMap.get(address).getMachine();
      return allMachines.indexOf(machine);
   }

   /**
    * Base class for locations.
    *
    * <p>Implements Comparable, but locations with equal capacity are equal, so they can't be used as map keys.</p>
    */
   public abstract static class Location implements Comparable<Location> {
      float totalCapacity;
      int nodeCount;
      float expectedPrimarySegments;
      float expectedOwnedSegments;

      abstract Collection<? extends Location> getChildren();

      abstract String getName();

      abstract String getFullName();

      float getCapacityPerNode() {
         return totalCapacity / nodeCount;
      }

      float getExpectedBackupSegments() {
         return expectedOwnedSegments - expectedPrimarySegments;
      }

      void collectNodes(Collection<Address> addressCollection) {
         for (Location location : getChildren()) {
            location.collectNodes(addressCollection);
         }
      }

      public Collection<String> getChildNames() {
         Collection<String> names = new ArrayList<>();
         for (Location location : getChildren()) {
            names.add(location.getName());
         }
         return names;
      }

      @Override
      public int compareTo(Location o) {
         // Sort descending by total capacity, ignore everything else
         return Float.compare(o.totalCapacity, totalCapacity);
      }

      @Override
      public String toString() {
         String name = getFullName();
         return String.format("%s * %f: %.2f+%.2f", name != null ? name : "/", totalCapacity, expectedPrimarySegments,
                              getExpectedBackupSegments());
      }
   }

   public static class Cluster extends Location {
      final List<Site> sites = new ArrayList<>();
      final Map<String, Site> siteMap = new HashMap<>();

      void addNode(String siteId, String rackId, String machineId, Address address, float capacityFactor) {
         Site site = siteMap.get(siteId);
         if (site == null) {
            site = new Site(this, siteId);
            sites.add(site);
            siteMap.put(siteId, site);
         }
         site.addNode(rackId, machineId, address, capacityFactor);
         totalCapacity += capacityFactor;
         nodeCount++;
      }

      Site getSite(String siteId) {
         return siteMap.get(siteId);
      }

      @Override
      Collection<Site> getChildren() {
         return sites;
      }

      @Override
      String getName() {
         return "cluster";
      }

      @Override
      String getFullName() {
         return "";
      }
   }

   public static class Site extends Location {
      final Cluster cluster;
      final String siteId;

      final List<Rack> racks = new ArrayList<>();
      final Map<String, Rack> rackMap = new HashMap<>();

      Site(Cluster cluster, String siteId) {
         this.cluster = cluster;
         this.siteId = siteId;
      }

      void addNode(String rackId, String machineId, Address address, float capacityFactor) {
         Rack rack = rackMap.get(rackId);
         if (rack == null) {
            rack = new Rack(this, rackId);
            racks.add(rack);
            rackMap.put(rackId, rack);
         }
         rack.addNode(machineId, address, capacityFactor);
         totalCapacity += capacityFactor;
         nodeCount++;
      }

      Rack getRack(String rackId) {
         return rackMap.get(rackId);
      }

      @Override
      Collection<Rack> getChildren() {
         return racks;
      }

      @Override
      String getName() {
         return siteId;
      }

      @Override
      String getFullName() {
         return siteId;
      }
   }

   public static class Rack extends Location {
      final Site site;
      final String rackId;

      final List<Machine> machines = new ArrayList<>();
      final Map<String, Machine> machineMap = new HashMap<>();

      Rack(Site site, String rackId) {
         this.site = site;
         this.rackId = rackId;
      }

      void addNode(String machineId, Address address, float capacityFactor) {
         Machine machine = machineMap.get(machineId);
         if (machine == null) {
            machine = new Machine(this, machineId);
            machines.add(machine);
            machineMap.put(machineId, machine);
         }
         machine.addNode(address, capacityFactor);
         totalCapacity += capacityFactor;
         nodeCount++;
      }

      Machine getMachine(String machineId) {
         return machineMap.get(machineId);
      }

      @Override
      Collection<Machine> getChildren() {
         return machines;
      }

      @Override
      String getName() {
         return rackId;
      }

      @Override
      String getFullName() {
         return rackId + '|' + site.siteId;
      }
   }

   public static class Machine extends Location {
      final Rack rack;
      final String machineId;

      final List<Node> nodes = new ArrayList<>();

      Machine(Rack rack, String machineId) {
         this.rack = rack;
         this.machineId = machineId;
      }

      void addNode(Address address, float capacityFactor) {
         nodes.add(new Node(this, address, capacityFactor));
         totalCapacity += capacityFactor;
         nodeCount++;
      }

      @Override
      Collection<Node> getChildren() {
         return nodes;
      }

      @Override
      String getName() {
         return machineId;
      }

      @Override
      String getFullName() {
         return machineId + '|' + rack.rackId + '|' + rack.site.siteId;
      }
   }

   public static class Node extends Location {
      final Machine machine;
      final Address address;

      Node(Machine machine, Address address, float capacityFactor) {
         this.machine = machine;
         this.address = address;
         this.totalCapacity = capacityFactor;
      }

      public Machine getMachine() {
         return machine;
      }

      public Rack getRack() {
         return machine.rack;
      }

      public Site getSite() {
         return machine.rack.site;
      }

      @Override
      Collection<Node> getChildren() {
         return Collections.singletonList(this);
      }

      @Override
      void collectNodes(Collection<Address> addressCollection) {
         addressCollection.add(address);
      }

      @Override
      String getName() {
         return address.toString();
      }

      @Override
      String getFullName() {
         return address.toString() + '|' + machine.machineId + '|' + machine.rack.rackId + '|' +
                machine.rack.site.siteId;
      }

      void addPrimarySegments(float segments) {
         expectedPrimarySegments += segments;
         machine.expectedPrimarySegments += segments;
         machine.rack.expectedPrimarySegments += segments;
         machine.rack.site.expectedPrimarySegments += segments;
         machine.rack.site.cluster.expectedPrimarySegments += segments;
      }

      void addOwnedSegments(float segments) {
         expectedOwnedSegments += segments;
         machine.expectedOwnedSegments += segments;
         machine.rack.expectedOwnedSegments += segments;
         machine.rack.site.expectedOwnedSegments += segments;
         machine.rack.site.cluster.expectedOwnedSegments += segments;
      }

      @Override
      public String toString() {
         return address.toString();
      }
   }
}
