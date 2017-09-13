package org.infinispan.server.test.util;

import org.jgroups.protocols.PARTITION_HANDLING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Controller that handles managing the cluster's partition handling. Using this controller, you can easily
 * split/merge cluster anyhow you want. It uses a custom JGroups protocol {@link PARTITION_HANDLING}, which
 * is inserted into servers jgroups stack during the test startup via XSLT and some Ant stuff in pom.xml.
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 */
public class PartitionHandlingController {

   private JGroupsProbeClient probeClient = new JGroupsProbeClient();

   StandaloneManagementClient[] clients;
   List<String> nodeNames;

   private static final String JGROUPS_QUERY_PREFIX = "op=PARTITION_HANDLING.";

   public PartitionHandlingController(StandaloneManagementClient[] clients) {
      this.clients = clients;
      nodeNames = new ArrayList<>();
      for (StandaloneManagementClient client : clients) {
         nodeNames.add(client.nodeName);
      }
   }

   /**
    * Splits the cluster into any partition you want and waits until it happens. If it doesn't, throws an exception.
    *
    * To split a 5-node cluster with servers A, B, C, D, E to partitions (A, B, C) and (D, E):
    * Set<String> part1 = new HashSet<>(Arrays.asList("A", "B", "C"));
    * Set<String> part2 = new HashSet<>(Arrays.asList("D", "E"));
    * partitionCluster(part1, part2);
    *
    * @param groups division of the partitions, see description of this method for details
    * @throws IllegalStateException when the desired partitioning didn't happen in specified timeout
    */
   public void partitionCluster(Set<String>... groups) {
      Map<String, Integer> nodeNamesToIndexes = new HashMap<>();
      int i = 1;

      for (String node: nodeNames) {
         nodeNamesToIndexes.put(node, i);
         String query = JGROUPS_QUERY_PREFIX + "setNodeIndex[" + node + "," + i + "]";
         probeClient.send(query);
         i++;
      }

      Map<String, String> oldViews = getCurrentViews();

      for (Set<String> group: groups) {
         for (String modifiedNode: group) {
            String query = JGROUPS_QUERY_PREFIX + "unsetAllowedNodes[" + modifiedNode + "]";
            probeClient.send(query);
            for (String otherNode: group) {
               query = JGROUPS_QUERY_PREFIX + "addAllowedNode[" + modifiedNode + "," + nodeNamesToIndexes.get(otherNode) + "]";
               probeClient.send(query);
            }
         }
      }

      waitForClusterToReform(oldViews);
   }

   public void heal() {
      partitionCluster(new HashSet<>(nodeNames));
   }

   /**
    * Wait for the nodes to realize that they lost/found some members.
    * We do that by waiting for the view to change on every node.
    */
   private void waitForClusterToReform(Map<String, String> oldViews) {
      final long TIMEOUT = 20000;
      final long REPEAT_SLEEP_TIME = 1000;

      long startTime = System.currentTimeMillis();
      long runningTime = startTime;
      while (runningTime - startTime < TIMEOUT) {
         Map<String, String> newViews = getCurrentViews();
         boolean allViewsChanged = true;
         for (String node: newViews.keySet()) {
            if (newViews.get(node).equalsIgnoreCase(oldViews.get(node))) {
               allViewsChanged = false;
            }
         }
         if (allViewsChanged) {
            return;
         }

         // some node haven't changed it's view yet, sleep for a minute
         try {
            Thread.sleep(REPEAT_SLEEP_TIME);
         } catch (InterruptedException e) {
            e.printStackTrace();
         }

         runningTime = System.currentTimeMillis();
      }

      throw new IllegalStateException("Cluster view should have changed withing " + TIMEOUT + " ms, but it didn't.");
   }

   /**
    * Returns the current view for every node.
    */
   private Map<String, String> getCurrentViews() {
      Map<String, String> views = new HashMap<>();

      for (StandaloneManagementClient client : clients) {
         views.put(client.nodeName, client.getInfinispanView());
      }

      return views;
   }

}
