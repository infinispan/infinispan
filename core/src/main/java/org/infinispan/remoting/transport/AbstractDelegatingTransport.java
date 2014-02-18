package org.infinispan.remoting.transport;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast) throws Exception {
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter, totalOrder, anycast);
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception {
      return actual.backupRemotely(backups, rpcCommand);
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
   public void checkTotalOrderSupported() {
      actual.checkTotalOrderSupported();
   }

   @Override
   public Log getLog() {
      return actual.getLog();
   }
}
