package org.infinispan.remoting.transport.jgroups;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
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
   private final Map<Address, ExtendedUUID> view;
   private final boolean isCoordinator;
   private final Address coordinator;

   ClusterView(int viewId, List<ExtendedUUID> members, ExtendedUUID self) {
      this.viewId = viewId;
      if (members.isEmpty()) {
         view = Map.of();
         isCoordinator = false;
         coordinator = null;
      } else {
         this.view = new LinkedHashMap<>();
         isCoordinator = Objects.equals(self, members.get(0));
         coordinator = JGroupsAddressCache.fromExtendedUUID(members.get(0));
         members.forEach(extendedUUID -> view.put(JGroupsAddressCache.fromExtendedUUID(extendedUUID), extendedUUID));

      }
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

   boolean contains(Address address) {
      return view.containsKey(address);
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
