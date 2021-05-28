package org.infinispan.remoting.transport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
@Scope(Scopes.GLOBAL)
public abstract class AbstractDelegatingTransport implements Transport {

   protected final Transport actual;

   protected AbstractDelegatingTransport(Transport actual) {
      this.actual = actual;
   }

   @Deprecated
   @Override
   public Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception {
      return actual.invokeRemotely(recipients, rpcCommand, mode, timeout, responseFilter, deliverOrder, anycast);
   }

   @Deprecated
   @Override
   public Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder, boolean anycast) throws Exception {
      return actual.invokeRemotely(rpcCommands, mode, timeout, usePriorityQueue, responseFilter, totalOrder, anycast);
   }

   @Deprecated
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
   public void sendToAll(ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      actual.sendToAll(rpcCommand, deliverOrder);
   }

   @Override
   public BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception {
      return actual.backupRemotely(backups, rpcCommand);
   }

   @Override
   public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteReplicateCommand<O> rpcCommand) {
      return actual.backupRemotely(backup, rpcCommand);
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
   public List<Address> getMembersPhysicalAddresses() {
      return actual.getMembersPhysicalAddresses();
   }

   @Override
   public boolean isMulticastCapable() {
      return actual.isMulticastCapable();
   }

   @Override
   public void checkCrossSiteAvailable() throws CacheConfigurationException {
      actual.checkCrossSiteAvailable();
   }

   @Override
   public String localSiteName() {
      return actual.localSiteName();
   }

   @Start
   @Override
   public void start() {
      actual.start();
   }

   @Stop
   @Override
   public void stop() {
      actual.stop();
   }

   @Override
   public int getViewId() {
      return actual.getViewId();
   }

   @Override
   public CompletableFuture<Void> withView(int expectedViewId) {
      return actual.withView(expectedViewId);
   }

   @Deprecated
   @Override
   public void waitForView(int viewId) throws InterruptedException {
      actual.waitForView(viewId);
   }

   @Override
   public Log getLog() {
      return actual.getLog();
   }

   public Transport getDelegate() {
      return actual;
   }

   @Override
   public Set<String> getSitesView() {
      return actual.getSitesView();
   }

   @Override
   public boolean isSiteCoordinator() {
      return actual.isSiteCoordinator();
   }

   @Override
   public Collection<Address> getSiteCoordinatorsAddress() {
      return actual.getSiteCoordinatorsAddress();
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                               long timeout, TimeUnit unit) {
      return actual.invokeCommand(target, command, collector, deliverOrder, timeout, unit);
   }

   @Override
   public <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                               ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                               long timeout, TimeUnit unit) {
      return actual.invokeCommand(targets, command, collector, deliverOrder, timeout, unit);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                                    DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      return actual.invokeCommandOnAll(command, collector, deliverOrder, timeout, unit);
   }

   @Override
   public <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                        ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                        long timeout, TimeUnit unit) {
      return actual.invokeCommandStaggered(targets, command, collector, deliverOrder, timeout, unit);
   }

   @Override
   public <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                Function<Address, ReplicableCommand> commandGenerator,
                                                ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                long timeout,
                                                TimeUnit timeUnit) {
      return actual.invokeCommands(targets, commandGenerator, collector, deliverOrder, timeout, timeUnit);
   }
}
