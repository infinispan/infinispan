package org.infinispan.remoting.transport.jgroups;

import java.util.List;
import java.util.Set;

import org.infinispan.commons.util.ImmutableHopscotchHashSet;
import org.infinispan.commons.util.Immutables;
import org.infinispan.remoting.transport.Address;

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
   private final List<Address> members;
   private final Set<Address> membersSet;
   private final Address coordinator;
   private final boolean isCoordinator;

   ClusterView(int viewId, List<Address> members, Address self) {
      this.viewId = viewId;
      this.members = Immutables.immutableListCopy(members);
      this.membersSet = new ImmutableHopscotchHashSet<>(members);
      if (!members.isEmpty()) {
         this.coordinator = members.get(0);
         this.isCoordinator = coordinator.equals(self);
      } else {
         this.coordinator = null;
         this.isCoordinator = false;
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
      return members;
   }

   public Set<Address> getMembersSet() {
      return membersSet;
   }

   public Address getCoordinator() {
      return coordinator;
   }

   public boolean isCoordinator() {
      return isCoordinator;
   }

   boolean contains(Address address) {
      return getMembersSet().contains(address);
   }

   @Override
   public String toString() {
      return coordinator + "|" + viewId + members;
   }
}
