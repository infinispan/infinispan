package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.metrics.impl.CustomMetricsSupplier;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.response.AutoStateTransferResponse;
import org.infinispan.xsite.response.AutoStateTransferResponseCollector;
import org.infinispan.xsite.statetransfer.StateTransferStatus;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.status.BringSiteOnlineResponse;
import org.infinispan.xsite.status.CacheMixedSiteStatus;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.SiteStatus;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.infinispan.xsite.status.TakeSiteOfflineResponse;

/**
 * A Managed bean exposing system administration operations for Cross-Site replication functionality (cache scope only)
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
@MBean(objectName = "XSiteAdmin", description = "Exposes tooling for handling backing up data to remote sites.")
public class XSiteAdminOperations implements CustomMetricsSupplier {

   public static final String ONLINE = "online";
   public static final String OFFLINE = "offline";
   public static final String SUCCESS = "ok";
   private static final Function<CacheMixedSiteStatus, String> DEFAULT_MIXED_MESSAGES = s -> "mixed, offline on nodes: " + s.getOffline();
   private static final Log log = LogFactory.getLog(XSiteAdminOperations.class);

   @Inject RpcManager rpcManager;
   @Inject XSiteStateTransferManager stateTransferManager;
   @Inject CommandsFactory commandsFactory;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject Authorizer authorizer;
   @Inject DistributionManager distributionManager;

   public static String siteStatusToString(SiteStatus status, Function<CacheMixedSiteStatus, String> mixedFunction) {
      if (status.isOffline()) {
         return OFFLINE;
      } else if (status.isOnline()) {
         return ONLINE;
      } else {
         assert status instanceof CacheMixedSiteStatus;
         return mixedFunction.apply((CacheMixedSiteStatus) status);
      }
   }

   public static String siteStatusToString(SiteStatus status) {
      return siteStatusToString(status, DEFAULT_MIXED_MESSAGES);
   }

   public Map<String, SiteStatus> clusterStatus() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      Map<String, Boolean> localNodeStatus = takeOfflineManager.status();

      return localNodeStatus.entrySet().stream()
            .map(entry -> Map.entry(entry.getKey(), SiteStatus.status(entry.getValue())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
   }

   @ManagedOperation(description = "Check whether the given backup site is offline or not.", displayName = "Check whether the given backup site is offline or not.")
   public String siteStatus(@Parameter(name = "site", description = "The name of the backup site") String site) {
      //also consider local node
      var siteState = takeOfflineManager.getSiteState(site);
      if (takeOfflineManager.getSiteState(site) == SiteState.NOT_FOUND) {
         return incorrectSiteName(site);
      }
      return siteState == SiteState.ONLINE ? ONLINE : OFFLINE;
   }

   /**
    * Gets the status of the nodes from a site
    *
    * @param site The name of the backup site
    * @return a Map&lt;String, String&gt; with the Address and the status of each node in the site.
    */
   public Map<Address, String> nodeStatus(String site) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      var state = takeOfflineManager.getSiteState(site);
      if (state == SiteState.NOT_FOUND) {
         throw new IllegalArgumentException(incorrectSiteName(site));
      }
      var result = state == SiteState.ONLINE ? ONLINE : OFFLINE;

      return distributionManager.getCacheTopology().getMembers()
            .stream()
            .collect(Collectors.toMap(CompletableFutures.identity(), address -> result));
   }

   @ManagedOperation(description = "Returns the the status(offline/online) of all the configured backup sites.", displayName = "Returns the the status(offline/online) of all the configured backup sites.")
   public String status() {
      Map<String, SiteStatus> statuses = clusterStatus();
      List<String> result = new ArrayList<>(statuses.size());
      statuses.forEach((site, status) -> result.add(site + "[" + siteStatusToString(status).toUpperCase() + "]"));
      return String.join("\n", result);
   }

   @ManagedOperation(description = "Takes this site offline in all nodes in the cluster.", displayName = "Takes this site offline in all nodes in the cluster.")
   public String takeSiteOffline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      TakeSiteOfflineResponse rsp = takeOfflineManager.takeSiteOffline(site);
      if (rsp == TakeSiteOfflineResponse.TSOR_NO_SUCH_SITE) {
         return incorrectSiteName(site);
      }
      return SUCCESS;
   }

   @ManagedOperation(description = "Amends the values for 'afterFailures' for the 'TakeOffline' functionality on all the nodes in the cluster.", displayName = "Amends the values for 'TakeOffline.afterFailures' on all the nodes in the cluster.")
   public String setTakeOfflineAfterFailures(
         @Parameter(name = "site", description = "The name of the backup site") String site,
         @Parameter(name = "afterFailures", description = "The number of failures after which the site will be taken offline") int afterFailures) {
      return takeOffline(site, afterFailures, null);
   }

   @ManagedOperation(description = "Amends the values for 'minTimeToWait' for the 'TakeOffline' functionality on all the nodes in the cluster.", displayName = "Amends the values for 'TakeOffline.minTimeToWait' on all the nodes in the cluster.")
   public String setTakeOfflineMinTimeToWait(
         @Parameter(name = "site", description = "The name of the backup site") String site,
         @Parameter(name = "minTimeToWait", description = "The minimum amount of time in milliseconds to wait before taking a site offline") long minTimeToWait) {
      return takeOffline(site, null, minTimeToWait);
   }

   @ManagedOperation(description = "Amends the values for 'TakeOffline' functionality on all the nodes in the cluster.", displayName = "Amends the values for 'TakeOffline' functionality on all the nodes in the cluster.")
   public String amendTakeOffline(
         @Parameter(name = "site", description = "The name of the backup site") String site,
         @Parameter(name = "afterFailures", description = "The number of failures after which the site will be taken offline") int afterFailures,
         @Parameter(name = "minTimeToWait", description = "The minimum amount of time in milliseconds to wait before taking a site offline") long minTimeToWait) {
      return takeOffline(site, afterFailures, minTimeToWait);
   }

   @ManagedOperation(description = "Returns the value of the 'minTimeToWait' for the 'TakeOffline' functionality.", displayName = "Returns the value of the 'minTimeToWait' for the 'TakeOffline' functionality.")
   public String getTakeOfflineMinTimeToWait(@Parameter(name = "site", description = "The name of the backup site") String site) {
      TakeOfflineConfiguration config = getTakeOfflineConfiguration(site);
      return config == null ? incorrectSiteName(site) : String.valueOf(config.minTimeToWait());
   }

   @ManagedOperation(description = "Returns the value of the 'afterFailures' for the 'TakeOffline' functionality.", displayName = "Returns the value of the 'afterFailures' for the 'TakeOffline' functionality.")
   public String getTakeOfflineAfterFailures(
         @Parameter(name = "site", description = "The name of the backup site") String site) {
      TakeOfflineConfiguration config = getTakeOfflineConfiguration(site);
      return config == null ? incorrectSiteName(site) : String.valueOf(config.afterFailures());
   }

   public TakeOfflineConfiguration getTakeOfflineConfiguration(String site) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return takeOfflineManager.getConfiguration(site);
   }

   public boolean checkSite(String site) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return takeOfflineManager.getSiteState(site) != SiteState.NOT_FOUND;
   }

   @ManagedOperation(description = "Brings the given site back online on all the cluster.", displayName = "Brings the given site back online on all the cluster.")
   public String bringSiteOnline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      BringSiteOnlineResponse rsp = takeOfflineManager.bringSiteOnline(site);
      if (rsp == BringSiteOnlineResponse.BSOR_NO_SUCH_SITE) {
         return incorrectSiteName(site);
      }
      return SUCCESS;
   }

   @ManagedOperation(displayName = "Push state to site",
         description = "Pushes the state of this cache to the remote site. " +
                       "The remote site will be bring back online",
         name = "pushState")
   public final String pushState(@Parameter(description = "The destination site name", name = "SiteName") String siteName) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      try {
         String status = bringSiteOnline(siteName);
         if (!SUCCESS.equals(status)) {
            return String.format("Unable to pushState to '%s'. %s", siteName, status);
         }
         stateTransferManager.startPushState(siteName);
      } catch (Throwable throwable) {
         log.xsiteAdminOperationError("pushState", siteName, throwable);
         return String.format("Unable to pushState to '%s'. %s", siteName, throwable.getLocalizedMessage());
      }
      return SUCCESS;
   }

   /**
    * For debugging only!
    */
   public final List<String> getRunningStateTransfer() {
      return stateTransferManager.getRunningStateTransfers();
   }

   @ManagedOperation(displayName = "Push State Status",
         description = "Shows a map with destination site name and the state transfer status.",
         name = "PushStateStatus")
   public final Map<String, String> getPushStateStatus() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      try {
         return stateTransferManager.getClusterStatus()
               .entrySet()
               .stream()
               .collect(Collectors.toMap(Map.Entry::getKey, entry -> StateTransferStatus.toText(entry.getValue())));
      } catch (Exception e) {
         return Collections.singletonMap(XSiteStateTransferManager.STATUS_ERROR, e.getLocalizedMessage());
      }
   }

   @ManagedOperation(displayName = "Clear State Status",
         description = "Clears the state transfer status.",
         name = "ClearPushStateStatus")
   public final String clearPushStateStatus() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return performOperation("clearPushStateStatus", "(local)", () -> stateTransferManager.clearClusterStatus());
   }

   @ManagedOperation(displayName = "Cancel Push State",
         description = "Cancels the push state to remote site.",
         name = "CancelPushState")
   public final String cancelPushState(@Parameter(description = "The destination site name", name = "SiteName") String siteName) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return performOperation("cancelPushState", siteName, () -> stateTransferManager.cancelPushState(siteName));
   }

   @ManagedOperation(displayName = "Cancel Receive State",
         description = "Cancels the push state to this site. All the state received from state transfer " +
                       "will be ignored.",
         name = "CancelReceiveState")
   public final String cancelReceiveState(@Parameter(description = "The sending site name", name = "SiteName") String siteName) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return performOperation("cancelReceiveState", siteName, () -> stateTransferManager.cancelReceive(siteName));
   }

   @ManagedOperation(displayName = "Sending Site Name",
         description = "Returns the site name from which this site is receiving state.",
         name = "SendingSiteName")
   public final String getSendingSiteName() {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      return stateTransferManager.getSendingSiteName();
   }

   public final CompletionStage<String> asyncGetStateTransferMode(String site) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      //check local first
      if (stateTransferManager.stateTransferMode(site) == XSiteStateTransferMode.MANUAL) {
         return CompletableFuture.completedFuture(XSiteStateTransferMode.MANUAL.toString());
      }
      ReplicableCommand cmd = commandsFactory.buildXSiteAutoTransferStatusCommand(site);
      AutoStateTransferResponseCollector collector = new AutoStateTransferResponseCollector(true, XSiteStateTransferMode.AUTO, false);
      return rpcManager.invokeCommandOnAll(cmd, collector, rpcManager.getSyncRpcOptions())
            .thenApply(AutoStateTransferResponse::stateTransferMode)
            .thenApply(Enum::toString);
   }

   @ManagedOperation(displayName = "State Transfer Mode",
         description = "Returns the cross-site replication state transfer mode.",
         name = "GetStateTransferMode")
   public final String getStateTransferMode(String site) {
      return CompletionStages.join(asyncGetStateTransferMode(site));
   }

   public CompletionStage<Boolean> asyncSetStateTransferMode(String site, String mode) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      XSiteStateTransferMode stateTransferMode = XSiteStateTransferMode.valueOf(mode);

      //update locally first
      if (!stateTransferManager.setAutomaticStateTransfer(site, stateTransferMode)) {
         //failed
         return CompletableFutures.completedFalse();
      }

      ReplicableCommand cmd = commandsFactory.buildXSiteSetStateTransferModeCommand(site, stateTransferMode);
      return rpcManager.invokeCommandOnAll(cmd, org.infinispan.remoting.transport.impl.VoidResponseCollector.ignoreLeavers(), rpcManager.getSyncRpcOptions())
            .thenApply(o -> Boolean.TRUE);
   }

   @ManagedOperation(displayName = "Sets State Transfer Mode",
         description = "Sets the cross-site state transfer mode.",
         name = "SetStateTransferMode")
   public final boolean setStateTransferMode(String site, String mode) {
      return CompletionStages.join(asyncSetStateTransferMode(site, mode));
   }

   private static String performOperation(String operationName, String siteName, Operation operation) {
      try {
         operation.execute();
      } catch (Throwable t) {
         log.xsiteAdminOperationError(operationName, siteName, t);
         return String.format("Unable to perform operation. Error=%s", t.getLocalizedMessage());
      }
      return SUCCESS;
   }

   private String takeOffline(String site, Integer afterFailures, Long minTimeToWait) {
      authorizer.checkPermission(AuthorizationPermission.ADMIN);
      if (takeOfflineManager.getSiteState(site) == SiteState.NOT_FOUND) {
         return incorrectSiteName(site);
      }

      // TODO [pruivo] use mutable attributes to update the take offline settings?
      // https://issues.redhat.com/browse/ISPN-16780
      CacheRpcCommand command = commandsFactory.buildXSiteAmendOfflineStatusCommand(site, afterFailures, minTimeToWait);
      XSiteResponse response = invokeOnAll(command, new VoidResponseCollector(clusterSize()));

      //also amend locally
      takeOfflineManager.amendConfiguration(site, afterFailures, minTimeToWait);

      if (response.errors.isEmpty()) {
         return SUCCESS;
      }
      return String.format("Could not amend for nodes: %s", response.errors);
   }

   private static String incorrectSiteName(String site) {
      return "Incorrect site name: " + site;
   }

   private XSiteResponse invokeOnAll(CacheRpcCommand command, ResponseCollector<XSiteResponse> responseCollector) {
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      CompletionStage<XSiteResponse> rsp = rpcManager.invokeCommandOnAll(command, responseCollector, rpcOptions);
      return rpcManager.blocking(rsp);
   }

   private int clusterSize() {
      return rpcManager.getTransport().getMembers().size();
   }

   @Override
   public Collection<MetricInfo> getCustomMetrics(GlobalMetricsConfiguration configuration) {
      return takeOfflineManager.getCustomMetrics(configuration);
   }

   private interface Operation {
      void execute() throws Throwable;
   }

   private record XSiteResponse(List<Address> errors) {
   }

   private static class VoidResponseCollector extends ValidResponseCollector<XSiteResponse> {

      private final List<Address> errors;

      private VoidResponseCollector(int expectedSize) {
         errors = new ArrayList<>(expectedSize);
      }

      @Override
      public XSiteResponse finish() {
         return new XSiteResponse(errors);
      }

      @Override
      protected XSiteResponse addValidResponse(Address sender, ValidResponse response) {
         //no-op
         return null;
      }

      @Override
      protected final XSiteResponse addTargetNotFound(Address sender) {
         //ignore leavers
         return null;
      }

      @Override
      protected final XSiteResponse addException(Address sender, Exception exception) {
         errors.add(sender);
         return null;
      }
   }

}
