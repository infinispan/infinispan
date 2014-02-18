package org.infinispan.xsite.statetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.concurrent.NotifyingFutureImpl;
import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl;

/**
 * {@link org.infinispan.xsite.statetransfer.XSiteStateTransferManager} implementation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferManagerImpl implements XSiteStateTransferManager {

   private final ConcurrentMap<String, XSiteStateTransferCollector> siteCollector;
   private RpcManager rpcManager;
   private Configuration configuration;
   private CommandsFactory commandsFactory;
   private ResponseGenerator responseGenerator;
   private ExecutorService asyncExecutor;

   public XSiteStateTransferManagerImpl() {
      siteCollector = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void inject(RpcManager rpcManager, Configuration configuration, CommandsFactory commandsFactory,
                      ResponseGenerator responseGenerator,
                      @ComponentName(value = ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncExecutor) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.commandsFactory = commandsFactory;
      this.responseGenerator = responseGenerator;
      this.asyncExecutor = asyncExecutor;
   }

   @Override
   public void notifyStatePushFinished(String siteName, Address node) throws Throwable {
      XSiteStateTransferCollector collector = siteCollector.get(siteName);
      if (collector == null) {
         return;
      }
      final XSiteBackup xSiteBackup = findSite(siteName);
      if (collector.confirmStateTransfer(node)) {
         siteCollector.remove(siteName);
         controlStateTransferOnRemoteSite(xSiteBackup, StateTransferControl.FINISH_RECEIVE);
      }
   }

   @Override
   public final void startPushState(String siteName) throws Throwable {
      //check site name first
      if (siteName == null) {
         throw new NullPointerException("Site name cannot be null!");
      }
      final XSiteBackup xSiteBackup = findSite(siteName);
      if (xSiteBackup == null) {
         throw new IllegalArgumentException("Site " + siteName + " not found!");
      }

      if (siteCollector.putIfAbsent(siteName, new XSiteStateTransferCollector(rpcManager.getMembers())) != null) {
         throw new Exception(format("X-Site state transfer to '%s' already started!", siteName));
      }

      try {
         controlStateTransferOnRemoteSite(xSiteBackup, StateTransferControl.START_RECEIVE);
         controlStateTransferOnLocalSite(StateTransferControl.START_SEND, siteName);
      } catch (Throwable throwable) {
         handleFailure(xSiteBackup);
         throw new Exception(throwable);
      }
   }

   @Override
   public List<String> getRunningStateTransfers() {
      return siteCollector.isEmpty() ? Collections.<String>emptyList() : new ArrayList<String>(siteCollector.keySet());
   }

   private void handleFailure(XSiteBackup xSiteBackup) {
      try {
         controlStateTransferOnLocalSite(StateTransferControl.CANCEL_SEND, xSiteBackup.getSiteName());
      } catch (Exception e) {
         //ignored
      }
      try {
         controlStateTransferOnRemoteSite(xSiteBackup, StateTransferControl.FINISH_RECEIVE);
      } catch (Throwable throwable) {
         //ignored
      }
   }

   private void controlStateTransferOnRemoteSite(XSiteBackup xSiteBackup, StateTransferControl control) throws Throwable {
      XSiteStateTransferControlCommand command = commandsFactory.buildXSiteStateTransferControlCommand(control, null);
      BackupResponse response = invokeRemotelyInRemoteSite(command, xSiteBackup);
      response.waitForBackupToFinish();
      if (!response.getFailedBackups().values().isEmpty()) {
         throw response.getFailedBackups().values().iterator().next();
      }
   }

   private void controlStateTransferOnLocalSite(StateTransferControl control, String siteName) throws Exception {
      XSiteStateTransferControlCommand command = commandsFactory.buildXSiteStateTransferControlCommand(control, siteName);
      for (Map.Entry<Address, Response> entry : invokeRemotelyInLocalSite(command).entrySet()) {
         if (entry.getValue() instanceof ExceptionResponse) {
            throw ((ExceptionResponse) entry.getValue()).getException();
         }
      }
   }

   private XSiteBackup findSite(String siteName) {
      SitesConfiguration sites = configuration.sites();
      for (BackupConfiguration bc : sites.allBackups()) {
         if (bc.site().equals(siteName)) {
            return new XSiteBackup(bc.site(), true, bc.stateTransfer().timeout());
         }
      }
      return null;
   }

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) throws Exception {
      commandsFactory.initializeReplicableCommand(command, false);
      final NotifyingNotifiableFuture<Object> remoteFuture = new NotifyingFutureImpl<Object>();
      rpcManager.invokeRemotelyInFuture(null, command, rpcManager.getDefaultRpcOptions(true, false), remoteFuture);
      final Future<Response> localFuture = asyncExecutor.submit(
            LocalInvocation.newInstance(responseGenerator, command, commandsFactory, rpcManager.getAddress()));
      final Map<Address, Response> responseMap = new HashMap<Address, Response>();
      responseMap.put(rpcManager.getAddress(), localFuture.get());
      //noinspection unchecked
      responseMap.putAll((Map<? extends Address, ? extends Response>) remoteFuture.get());
      return responseMap;
   }

   private BackupResponse invokeRemotelyInRemoteSite(XSiteReplicateCommand command, XSiteBackup xSiteBackup) throws Exception {
      return rpcManager.getTransport().backupRemotely(Collections.singletonList(xSiteBackup), command);
   }

}
