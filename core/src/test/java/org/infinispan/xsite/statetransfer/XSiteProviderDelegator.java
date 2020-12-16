package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

/**
 * {@link org.infinispan.xsite.statetransfer.XSiteStateProvider} delegator. Mean to be overridden. For test purpose
 * only!
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteProviderDelegator implements XSiteStateProvider {

   protected final XSiteStateProvider xSiteStateProvider;

   protected XSiteProviderDelegator(XSiteStateProvider xSiteStateProvider) {
      this.xSiteStateProvider = xSiteStateProvider;
   }

   @Override
   public void startStateTransfer(String siteName, Address requestor, int minTopologyId) {
      xSiteStateProvider.startStateTransfer(siteName, requestor, minTopologyId);
   }

   @Override
   public void cancelStateTransfer(String siteName) {
      xSiteStateProvider.cancelStateTransfer(siteName);
   }

   @Override
   public Collection<String> getCurrentStateSending() {
      return xSiteStateProvider.getCurrentStateSending();
   }

   @Override
   public Collection<String> getSitesMissingCoordinator(Collection<Address> currentMembers) {
      return xSiteStateProvider.getSitesMissingCoordinator(currentMembers);
   }

   @Override
   public void notifyStateTransferEnd(String siteName, Address origin, boolean statusOk) {
      xSiteStateProvider.notifyStateTransferEnd(siteName, origin, statusOk);
   }

   @Override
   public CommandsFactory getCommandsFactory() {
      return xSiteStateProvider.getCommandsFactory();
   }

   @Override
   public RpcManager getRpcManager() {
      return xSiteStateProvider.getRpcManager();
   }

   @Override
   public ScheduledExecutorService getScheduledExecutorService() {
      return xSiteStateProvider.getScheduledExecutorService();
   }

   @Override
   public Executor getExecutor() {
      return xSiteStateProvider.getExecutor();
   }
}
