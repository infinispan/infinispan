package org.infinispan.xsite.statetransfer;

import org.infinispan.remoting.transport.Address;

import java.util.Collection;

/**
 * {@link org.infinispan.xsite.statetransfer.XSiteStateProvider} delegator. Mean to be overridden. For test purpose
 * only!
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteProviderDelegator implements XSiteStateProvider {

   private final XSiteStateProvider xSiteStateProvider;

   public XSiteProviderDelegator(XSiteStateProvider xSiteStateProvider) {
      this.xSiteStateProvider = xSiteStateProvider;
   }

   @Override
   public void startStateTransfer(String siteName, Address requestor) {
      xSiteStateProvider.startStateTransfer(siteName, requestor);
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      xSiteStateProvider.cancelStateTransfer(siteName);
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      return xSiteStateProvider.getCurrentStateSending();
   }
}
