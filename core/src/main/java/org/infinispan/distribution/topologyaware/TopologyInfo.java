/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.topologyaware;

import java.util.ArrayList;
import java.util.Collection;
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
   int allNodesCount;

   public TopologyInfo(Collection<Address> members) {
      for (Address node : members) {
         addTopology(node);
      }
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
      allNodesCount++;
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
      return allNodesCount;
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

   public double computeMaxSegmentsForNode(int numSegments, double numCopies, int nodesCount) {
      if (nodesCount < numCopies) {
         return numSegments;
      } else {
         // The number of segment copies on each node should be the same
         return numCopies * numSegments / nodesCount;
      }
   }

   public double computeMaxSegmentsForMachine(int numSegments, double numCopies, Collection<Machine> machines,
                                              Machine machine) {
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
      return computeMaxSegmentsForNode(numSegments, copiesPerMachine, machine.nodes.size());
   }

   public double computeMaxSegmentsForRack(int numSegments, double numCopies, Collection<Rack> racks, Rack rack,
                                           Machine machine) {
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
         return computeMaxSegmentsForNode(numSegments, copiesPerRack, rack.nodes.size());
      } else {
         return computeMaxSegmentsForMachine(numSegments, copiesPerRack, rack.machines.values(), machine);
      }
   }

   public double computeMaxSegmentsForSite(int numSegments, double numCopies, Collection<Site> sites,
                                           Site site, Rack rack, Machine machine) {
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
         return computeMaxSegmentsForNode(numSegments, copiesPerSite, site.nodes.size());
      } else {
         return computeMaxSegmentsForRack(numSegments, copiesPerSite, site.racks.values(), rack, machine);
      }
   }


   public int computeMaxSegments(int numSegments, int numOwners, Address node) {
      TopologyAwareAddress taa = (TopologyAwareAddress) node;
      String siteId = taa.getSiteId();
      String rackId = taa.getRackId();
      String machineId = taa.getMachineId();

      Site site = allSites.get(siteId);
      Rack rack = site.racks.get(rackId);
      Machine machine = rack.machines.get(machineId);

      double maxSegments;
      if (numOwners == 1) {
         maxSegments = computeMaxSegmentsForNode(numSegments, numOwners, allNodesCount);
      } else if (getAllNodesCount() <= numOwners) {
         maxSegments = numSegments;
      } else if (getAllMachinesCount() <= numOwners) {
         maxSegments = computeMaxSegmentsForMachine(numSegments, numOwners, allMachines, machine);
      } else if (getAllRacksCount() <= numOwners) {
         maxSegments = computeMaxSegmentsForRack(numSegments, numOwners, allRacks, rack, machine);
      } else {
         maxSegments = computeMaxSegmentsForSite(numSegments, numOwners, allSites.values(), site, rack, machine);
      }
      return (int) Math.ceil(maxSegments);
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
