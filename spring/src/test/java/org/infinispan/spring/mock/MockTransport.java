package org.infinispan.spring.mock;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class MockTransport implements Transport {

   @Override
   public Map<Address, Response> invokeRemotely(final Collection<Address> recipients,
                                                final ReplicableCommand rpcCommand, final ResponseMode mode, final long timeout,
                                                final boolean usePriorityQueue, final ResponseFilter responseFilter, final boolean totalOrder, final boolean anycast) throws Exception {
      return null;
   }

   @Override
   public boolean isCoordinator() {
      return false;
   }

   @Override
   public Address getCoordinator() {
      return null;
   }

   @Override
   public Address getAddress() {
      return null;
   }

   @Override
   public List<Address> getPhysicalAddresses() {
      return null;
   }

   @Override
   public List<Address> getMembers() {
      return null;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

   @Override
   public int getViewId() {
      return 0;
   }

   @Override
   public Log getLog() {
      return null;
   }

   @Override
   public void checkTotalOrderSupported() {
   }

   @Override
   public boolean isMulticastCapable() {
      return false;
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, ReplicableCommand rpcCommand) throws Exception {
      return null;
   }
}
