package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * It collects the acknowledgements sent from local site member to signal the ending of the state sent.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public final class XSiteStateTransferCollector {

   private final Set<Address> collector;

   public XSiteStateTransferCollector(Collection<Address> confirmationPending) {
      if (confirmationPending == null) {
         throw new NullPointerException("Pending confirmations must be non-null.");
      } else if (confirmationPending.isEmpty()) {
         throw new IllegalArgumentException("Pending confirmations must be non-empty.");
      }
      this.collector = new HashSet<Address>(confirmationPending);
   }

   public boolean confirmStateTransfer(Address node) {
      synchronized (collector) {
         return collector.remove(node) && collector.isEmpty();
      }
   }

   public boolean updateMembers(Collection<Address> members) {
      synchronized (collector) {
         return collector.retainAll(members) && collector.isEmpty();
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XSiteStateTransferCollector that = (XSiteStateTransferCollector) o;

      return collector.equals(that.collector);
   }

   @Override
   public int hashCode() {
      return collector.hashCode();
   }

   @Override
   public String toString() {
      return "XSiteStateTransferCollector{" +
            "collector=" + collector +
            '}';
   }
}
