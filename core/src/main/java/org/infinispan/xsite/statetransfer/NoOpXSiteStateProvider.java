package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * A no-op implementation of {@link XSiteStateProvider}.
 * <p>
 * This class is used when cross-site replication is disabled.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Scope(value = Scopes.NAMED_CACHE)
public class NoOpXSiteStateProvider implements XSiteStateProvider {

   private static final NoOpXSiteStateProvider INSTANCE = new NoOpXSiteStateProvider();

   private NoOpXSiteStateProvider() {
   }

   public static NoOpXSiteStateProvider getInstance() {
      return INSTANCE;
   }

   @Override
   public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
      // no-op
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      // no-op
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      return Collections.emptyList();
   }

   @Override
   public Collection<String> getSitesMissingCoordinator(Collection<Address> currentMembers) {
      return Collections.emptyList();
   }

   @Override
   public String toString() {
      return "NoOpXSiteStateProvider{}";
   }
}
