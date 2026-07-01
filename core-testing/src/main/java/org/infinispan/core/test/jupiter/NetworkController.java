package org.infinispan.core.test.jupiter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MutableDigest;

/**
 * Controls network communication between cluster nodes for failure injection.
 * <p>
 * Provides capabilities to:
 * <ul>
 *   <li>Isolate individual nodes (drop all messages to/from a node)</li>
 *   <li>Create network partitions (split cluster into groups)</li>
 *   <li>Merge partitions back together</li>
 * </ul>
 * <p>
 * Obtained via {@link InfinispanContext#network()}.
 *
 * <h3>Example: Isolate a single node</h3>
 * <pre>{@code
 * ctx.network().isolate(2);         // node 2 can no longer communicate
 * // ... test failure scenario ...
 * ctx.network().restore(2);         // re-establish communication
 * }</pre>
 *
 * <h3>Example: Create a network partition</h3>
 * <pre>{@code
 * ctx.network().partition(new int[]{0, 1}, new int[]{2, 3});
 * // ... test partition handling ...
 * ctx.network().merge();
 * }</pre>
 *
 * @since 16.2
 */
public class NetworkController {

   private final List<EmbeddedCacheManager> managers;
   private final AtomicLong viewIdCounter = new AtomicLong();
   private boolean partitioned;

   NetworkController(List<EmbeddedCacheManager> managers) {
      this.managers = managers;
      // Start view IDs above current values
      long maxViewId = 0;
      for (EmbeddedCacheManager manager : managers) {
         JChannel ch = extractChannel(manager);
         long vid = ch.getView().getViewId().getId();
         if (vid > maxViewId) maxViewId = vid;
      }
      viewIdCounter.set(maxViewId + 10);
   }

   /**
    * Isolates the node at the given index by dropping all inbound and outbound messages.
    * <p>
    * The isolated node remains running but cannot communicate with any other node.
    * Other nodes will eventually detect the failure through FD protocols.
    * Use {@link #restore(int)} to re-establish communication.
    *
    * @param nodeIndex the index of the node to isolate
    */
   public void isolate(int nodeIndex) {
      JChannel channel = extractChannel(managers.get(nodeIndex));
      DISCARD discard = findDiscard(channel);
      if (discard == null) {
         discard = new DISCARD();
         discard.excludeItself(false);
         try {
            channel.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
         } catch (Exception e) {
            throw new RuntimeException("Failed to insert DISCARD protocol", e);
         }
      }
      discard.discardAll(true);
   }

   /**
    * Restores communication for a previously isolated node.
    * <p>
    * Removes the DISCARD protocol and waits for the node to rejoin the cluster view.
    *
    * @param nodeIndex the index of the node to restore
    */
   public void restore(int nodeIndex) {
      JChannel channel = extractChannel(managers.get(nodeIndex));
      DISCARD discard = findDiscard(channel);
      if (discard != null) {
         channel.getProtocolStack().removeProtocol(DISCARD.class);
      }
   }

   /**
    * Splits the cluster into network partitions.
    * <p>
    * Each partition is defined by an array of node indices. Messages between
    * nodes in different partitions are dropped, and each partition gets its
    * own cluster view. Every node must appear in exactly one partition.
    *
    * <pre>{@code
    * // Split a 4-node cluster into two halves
    * ctx.network().partition(new int[]{0, 1}, new int[]{2, 3});
    *
    * // Split into three groups
    * ctx.network().partition(new int[]{0}, new int[]{1}, new int[]{2, 3});
    * }</pre>
    *
    * @param groups arrays of node indices, each array defining one partition
    * @throws IllegalArgumentException if nodes are missing or duplicated
    */
   public void partition(int[]... groups) {
      validatePartitionGroups(groups);

      // For each group, insert DISCARD to drop messages from nodes outside the group
      for (int[] group : groups) {
         List<org.jgroups.Address> outsideMembers = outsideAddresses(group, groups);
         for (int nodeIdx : group) {
            JChannel channel = extractChannel(managers.get(nodeIdx));
            DISCARD discard = findDiscard(channel);
            if (discard == null) {
               discard = new DISCARD();
               discard.excludeItself(true);
               try {
                  channel.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
               } catch (Exception e) {
                  throw new RuntimeException("Failed to insert DISCARD protocol", e);
               }
            }
            discard.resetIgnoredMembers();
            for (org.jgroups.Address addr : outsideMembers) {
               discard.addIgnoreMember(addr);
            }
         }
      }

      // Install a new view in each partition
      for (int[] group : groups) {
         installPartitionView(group);
      }

      partitioned = true;
   }

   /**
    * Merges all partitions back together.
    * <p>
    * Removes all DISCARD protocols, installs a merge view on all nodes,
    * and waits for the cluster to stabilize.
    */
   public void merge() {
      if (!partitioned) return;

      // Collect current sub-views before merge
      List<View> subViews = new ArrayList<>();
      for (EmbeddedCacheManager manager : managers) {
         JChannel ch = extractChannel(manager);
         View currentView = ch.getView();
         boolean alreadyPresent = subViews.stream()
               .anyMatch(v -> v.getMembers().equals(currentView.getMembers()));
         if (!alreadyPresent) {
            subViews.add(currentView);
         }
      }

      // Remove all DISCARD protocols
      for (EmbeddedCacheManager manager : managers) {
         JChannel channel = extractChannel(manager);
         DISCARD discard = findDiscard(channel);
         if (discard != null) {
            channel.getProtocolStack().removeProtocol(DISCARD.class);
         }
      }

      // Garbage collect old messages to avoid retransmission storms
      for (EmbeddedCacheManager manager : managers) {
         JChannel ch = extractChannel(manager);
         STABLE stable = ch.getProtocolStack().findProtocol(STABLE.class);
         if (stable != null) {
            stable.gc();
         }
      }

      // Build the merge view with all members
      List<org.jgroups.Address> allAddresses = managers.stream()
            .map(m -> extractChannel(m).getAddress())
            .collect(Collectors.toList());

      MutableDigest digest = new MutableDigest(allAddresses.toArray(new org.jgroups.Address[0]));
      for (EmbeddedCacheManager manager : managers) {
         GMS gms = extractChannel(manager).getProtocolStack().findProtocol(GMS.class);
         digest.merge(gms.getDigest());
      }

      long newViewId = viewIdCounter.incrementAndGet();
      MergeView mergeView = new MergeView(
            new ViewId(allAddresses.get(0), newViewId),
            allAddresses,
            subViews);

      for (EmbeddedCacheManager manager : managers) {
         GMS gms = extractChannel(manager).getProtocolStack().findProtocol(GMS.class);
         gms.installView(mergeView, digest);
      }

      partitioned = false;
   }

   /**
    * Drops messages between two specific nodes without creating full partitions.
    * <p>
    * This is a targeted link failure — other communication paths remain intact.
    *
    * @param nodeA first node index
    * @param nodeB second node index
    */
   public void dropMessagesBetween(int nodeA, int nodeB) {
      addIgnore(nodeA, nodeB);
      addIgnore(nodeB, nodeA);
   }

   /**
    * Restores messages between two specific nodes.
    *
    * @param nodeA first node index
    * @param nodeB second node index
    */
   public void restoreMessagesBetween(int nodeA, int nodeB) {
      removeIgnore(nodeA, nodeB);
      removeIgnore(nodeB, nodeA);
   }

   void reset() {
      if (partitioned) {
         merge();
      }
      // Remove any remaining DISCARD protocols
      for (EmbeddedCacheManager manager : managers) {
         JChannel channel = extractChannel(manager);
         DISCARD discard = findDiscard(channel);
         if (discard != null) {
            channel.getProtocolStack().removeProtocol(DISCARD.class);
         }
      }
   }

   private void addIgnore(int fromNode, int toNode) {
      JChannel channel = extractChannel(managers.get(fromNode));
      DISCARD discard = findDiscard(channel);
      if (discard == null) {
         discard = new DISCARD();
         discard.excludeItself(true);
         try {
            channel.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
         } catch (Exception e) {
            throw new RuntimeException("Failed to insert DISCARD protocol", e);
         }
      }
      org.jgroups.Address targetAddr = extractChannel(managers.get(toNode)).getAddress();
      discard.addIgnoreMember(targetAddr);
   }

   private void removeIgnore(int fromNode, int toNode) {
      JChannel channel = extractChannel(managers.get(fromNode));
      DISCARD discard = findDiscard(channel);
      if (discard != null) {
         org.jgroups.Address targetAddr = extractChannel(managers.get(toNode)).getAddress();
         discard.removeIgnoredMember(targetAddr);
      }
   }

   private void installPartitionView(int[] group) {
      List<org.jgroups.Address> viewMembers = new ArrayList<>(group.length);
      for (int idx : group) {
         viewMembers.add(extractChannel(managers.get(idx)).getAddress());
      }

      long newViewId = viewIdCounter.incrementAndGet();
      View view = View.create(viewMembers.get(0), newViewId,
            viewMembers.toArray(new org.jgroups.Address[0]));

      for (int idx : group) {
         GMS gms = extractChannel(managers.get(idx)).getProtocolStack().findProtocol(GMS.class);
         gms.installView(view);
      }
   }

   private void validatePartitionGroups(int[][] groups) {
      int totalNodes = managers.size();
      boolean[] seen = new boolean[totalNodes];
      for (int[] group : groups) {
         for (int idx : group) {
            if (idx < 0 || idx >= totalNodes) {
               throw new IllegalArgumentException("Node index " + idx + " out of range [0, " + totalNodes + ")");
            }
            if (seen[idx]) {
               throw new IllegalArgumentException("Node " + idx + " appears in multiple partitions");
            }
            seen[idx] = true;
         }
      }
      for (int i = 0; i < totalNodes; i++) {
         if (!seen[i]) {
            throw new IllegalArgumentException("Node " + i + " is not assigned to any partition");
         }
      }
   }

   private List<org.jgroups.Address> outsideAddresses(int[] group, int[][] allGroups) {
      List<org.jgroups.Address> outside = new ArrayList<>();
      int[] groupSorted = group.clone();
      Arrays.sort(groupSorted);
      for (int[] other : allGroups) {
         if (other == group) continue;
         for (int idx : other) {
            outside.add(extractChannel(managers.get(idx)).getAddress());
         }
      }
      return outside;
   }

   static JChannel extractChannel(EmbeddedCacheManager manager) {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(manager);
      Transport transport = gcr.getComponent(Transport.class);
      if (transport instanceof JGroupsTransport jgt) {
         return jgt.getChannel();
      }
      throw new IllegalStateException("Transport is not JGroupsTransport: " + transport);
   }

   private static DISCARD findDiscard(JChannel channel) {
      return channel.getProtocolStack().findProtocol(DISCARD.class);
   }
}
