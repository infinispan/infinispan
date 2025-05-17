package org.infinispan.remoting.transport.jgroups;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;
import org.jgroups.util.ExtendedUUID;

/**
 * Information about the JGroups cluster.
 *
 * @author Dan Berindei
 * @since 9.1
 */
public class ClusterView {
   static final int INITIAL_VIEW_ID = -1;
   static final int FINAL_VIEW_ID = Integer.MAX_VALUE;

   private final int viewId;
   private final Map<JGroupsAddress, ExtendedUUID> view;
   private final boolean isCoordinator;
   private final Address coordinator;
   private final NodeVersion oldestMember;
   private final boolean mixedVersionCluster;

   ClusterView(int viewId, List<ExtendedUUID> members, ExtendedUUID self, NodeVersion version) {
      this.viewId = viewId;
      var oldestVersion = version;
      var mixedVersionCluster = false;
      if (members.isEmpty()) {
         view = Map.of();
         isCoordinator = false;
         coordinator = null;
      } else {
         this.view = new LinkedHashMap<>();
         isCoordinator = Objects.equals(self, members.get(0));
         coordinator = JGroupsAddressCache.fromExtendedUUID(members.get(0));

         for (ExtendedUUID member : members) {
            var address = JGroupsAddressCache.fromExtendedUUID(member);
            view.put(address, member);

            var v = address.getVersion();
            if (!v.equals(version)) {
               mixedVersionCluster = true;

               if (v.lessThan(oldestVersion))
                  oldestVersion = v;
            }
         }
      }
      this.oldestMember = oldestVersion;
      this.mixedVersionCluster = mixedVersionCluster;
   }

   public int getViewId() {
      return viewId;
   }

   public boolean isViewIdAtLeast(int expectedViewId) {
      return expectedViewId <= viewId;
   }

   public boolean isStopped() {
      return viewId == FINAL_VIEW_ID;
   }

   public List<Address> getMembers() {
      return List.copyOf(view.keySet());
   }

   public Set<Address> getMembersSet() {
      return Collections.unmodifiableSet(view.keySet());
   }

   public Address getCoordinator() {
      return coordinator;
   }

   public boolean isCoordinator() {
      return isCoordinator;
   }

   public NodeVersion getOldestMember() {
      return oldestMember;
   }

   public boolean isMixedVersionCluster() {
      return mixedVersionCluster;
   }

   @Override
   public String toString() {
      return coordinator + "|" + viewId + view.keySet();
   }

   public ExtendedUUID getAddressFromView(Address address) {
      assert address instanceof JGroupsAddress;
      return view.get(address);
   }
}
