package org.infinispan.remoting.transport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Designed to be overwrite.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractDelegatingTransport implements Transport {

   protected final Transport actual;

   protected AbstractDelegatingTransport(Transport actual) {
      this.actual = actual;
   }

   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      beforeInvokeRemotely(rpcCommand);
      Map<Address, Response> result = actual.invokeRemotely(recipients, rpcCommand, mode, timeout, responseFilter, deliverOrder, anycast);
      return afterInvokeRemotely(rpcCommand, result);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast) throws Exception {
      return actual.invokeRemotely(rpcCommands, mode, timeout, usePriorityQueue, responseFilter, totalOrder, anycast);
   }

   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      return actual.invokeRemotely(rpcCommands, mode, timeout, responseFilter, deliverOrder, anycast);
   }

   @Override
   public CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                        ReplicableCommand rpcCommand,
                                                                        ResponseMode mode, long timeout,
                                                                        ResponseFilter responseFilter,
                                                                        DeliverOrder deliverOrder,
                                                                        boolean anycast) throws Exception {
      return actual.invokeRemotelyAsync(recipients, rpcCommand, mode, timeout, responseFilter, deliverOrder, anycast);
   }

   @Override
   public void sendTo(Address destination, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      actual.sendTo(destination, rpcCommand, deliverOrder);
   }

   @Override
   public void sendToMany(Collection<Address> destinations, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      actual.sendToMany(destinations, rpcCommand, deliverOrder);
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception {
      beforeBackupRemotely(rpcCommand);
      BackupResponse response = actual.backupRemotely(backups, rpcCommand);
      return afterBackupRemotely(rpcCommand, response);
   }

   @Override
   public boolean isCoordinator() {
      return actual.isCoordinator();
   }

   @Override
   public Address getCoordinator() {
      return actual.getCoordinator();
   }

   @Override
   public Address getAddress() {
      return actual.getAddress();
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      return actual.getPhysicalAddresses();
   }

   @Override
   public List<Address> getMembers() {
      return actual.getMembers();
   }

   @Override
   public boolean isMulticastCapable() {
      return actual.isMulticastCapable();
   }

   @Override
   public void start() {
      actual.start();
   }

   @Override
   public void stop() {
      actual.stop();
   }

   @Override
   public int getViewId() {
      return actual.getViewId();
   }

   @Override
   public void waitForView(int viewId) throws InterruptedException {
      actual.waitForView(viewId);
   }

   @Override
   public void checkTotalOrderSupported() {
      actual.checkTotalOrderSupported();
   }

   @Override
   public Log getLog() {
      return actual.getLog();
   }

   public Transport getDelegate() {
      return actual;
   }

   /**
    * method invoked before a remote invocation.
    *
    * @param command the command to be invoked remotely
    */
   protected void beforeInvokeRemotely(ReplicableCommand command) {
      //no-op by default
   }

   /**
    * method invoked after a successful remote invocation.
    *
    * @param command     the command invoked remotely.
    * @param responseMap can be null if not response is expected.
    * @return the new response map
    */
   protected Map<Address, Response> afterInvokeRemotely(ReplicableCommand command, Map<Address, Response> responseMap) {
      return responseMap;
   }

   /**
    * method invoked before a backup remote invocation.
    *
    * @param command the command to be invoked remotely
    */
   protected void beforeBackupRemotely(XSiteReplicateCommand command) {
      //no-op by default
   }

   /**
    * method invoked after a successful backup remote invocation.
    *
    * @param command  the command invoked remotely.
    * @param response can be null if not response is expected.
    * @return the new response map
    */
   protected BackupResponse afterBackupRemotely(ReplicableCommand command, BackupResponse response) {
      return response;
   }
}
