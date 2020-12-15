package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.StateTransferStatus;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.status.BringSiteOnlineResponse;
import org.infinispan.xsite.status.CacheMixedSiteStatus;
import org.infinispan.xsite.status.CacheSiteStatusBuilder;
import org.infinispan.xsite.status.SiteState;
import org.infinispan.xsite.status.SiteStatus;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.infinispan.xsite.status.TakeSiteOfflineResponse;

/**
 * Managed bean exposing sys admin operations for Cross-Site replication functionality.
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
@MBean(objectName = "XSiteAdmin", description = "Exposes tooling for handling backing up data to remote sites.")
public class XSiteAdminOperations {

   public static final String ONLINE = "online";
   public static final String FAILED = "failed";
   public static final String OFFLINE = "offline";
   public static final String SUCCESS = "ok";
   private static final Function<CacheMixedSiteStatus, String> DEFAULT_MIXED_MESSAGES = s -> "mixed, offline on nodes: " + s.getOffline();
   private static final Log log = LogFactory.getLog(XSiteAdminOperations.class);

   @Inject RpcManager rpcManager;
   @Inject XSiteStateTransferManager stateTransferManager;
   @Inject CommandsFactory commandsFactory;
   @Inject TakeOfflineManager takeOfflineManager;
   @Inject AuthorizationHelper authorizationHelper;

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
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      Map<String, Boolean> localNodeStatus = takeOfflineManager.status();
      CacheRpcCommand command = commandsFactory.buildXSiteStatusCommand();
      XSiteResponse<Map<String, Boolean>> response = invokeOnAll(command,
            new PerSiteBooleanResponseCollector(clusterSize()));
      if (response.hasErrors()) {
         throw new CacheException("Unable to check cluster state for members: " + response.getErrors());
      }
      //site name => online/offline/mixed
      Map<String, CacheSiteStatusBuilder> perSiteBuilder = new HashMap<>();
      for (Map.Entry<String, Boolean> entry : localNodeStatus.entrySet()) {
         CacheSiteStatusBuilder builder = new CacheSiteStatusBuilder();
         builder.addMember(rpcManager.getAddress(), entry.getValue());
         perSiteBuilder.put(entry.getKey(), builder);
      }

      response.forEach((address, sites) -> {
         for (Map.Entry<String, Boolean> site : sites.entrySet()) {
            CacheSiteStatusBuilder builder = perSiteBuilder.get(site.getKey());
            if (builder == null) {
               throw new IllegalStateException("Site " + site.getKey() + " not defined in all the cluster members");
            }
            builder.addMember(address, site.getValue());
         }
      });

      Map<String, SiteStatus> result = new HashMap<>();
      perSiteBuilder.forEach((site, builder) -> result.put(site, builder.build()));
      return result;
   }

   @ManagedOperation(description = "Check whether the given backup site is offline or not.", displayName = "Check whether the given backup site is offline or not.")
   public String siteStatus(@Parameter(name = "site", description = "The name of the backup site") String site) {
      //also consider local node
      if (takeOfflineManager.getSiteState(site) == SiteState.NOT_FOUND) {
         return incorrectSiteName(site);
      }

      Map<Address, String> statuses = nodeStatus(site);
      List<Address> online = new ArrayList<>(statuses.size());
      List<Address> offline = new ArrayList<>(statuses.size());
      List<Address> failed = new ArrayList<>(statuses.size());
      statuses.forEach((a, s) -> {
         if (s.equals(FAILED)) failed.add(a);
         if (s.equals(OFFLINE)) offline.add(a);
         if (s.equals(ONLINE)) online.add(a);
      });

      if (!failed.isEmpty()) return rpcError(failed, "Could not query nodes ");
      if (offline.isEmpty()) return ONLINE;
      if (online.isEmpty()) return OFFLINE;

      return "Site appears online on nodes:" + online + " and offline on nodes: " + offline;
   }

   /**
    * Obtain the status of the nodes from a site
    *
    * @param site The name of the backup site
    * @return a Map&lt;String, String&gt; with the Address and the status of each node in the site
    */
   public Map<Address, String> nodeStatus(String site) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      SiteState state = takeOfflineManager.getSiteState(site);
      if (state == SiteState.NOT_FOUND) {
         throw new IllegalArgumentException(incorrectSiteName(site));
      }

      CacheRpcCommand command = commandsFactory.buildXSiteOfflineStatusCommand(site);
      XSiteResponse<Boolean> response = invokeOnAll(command, new XSiteStatusResponseCollector(clusterSize()));
      Map<Address, String> statusMap = new HashMap<>();

      response.forEachError(address -> statusMap.put(address, FAILED));
      response.forEach((address, offline) -> statusMap.put(address, offline ? OFFLINE : ONLINE));
      statusMap.put(rpcManager.getAddress(), state == SiteState.OFFLINE ? OFFLINE : ONLINE);
      return statusMap;
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
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      TakeSiteOfflineResponse rsp = takeOfflineManager.takeSiteOffline(site);
      if (rsp == TakeSiteOfflineResponse.NO_SUCH_SITE) {
         return incorrectSiteName(site);
      }

      CacheRpcCommand command = commandsFactory.buildXSiteTakeOfflineCommand(site);
      XSiteResponse<Void> response = invokeOnAll(command, new VoidResponseCollector(clusterSize()));

      String prefix = "Could not take the site offline on nodes:";
      return returnFailureOrSuccess(response.getErrors(), prefix);
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
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      return takeOfflineManager.getConfiguration(site);
   }

   public boolean checkSite(String site) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      return takeOfflineManager.getSiteState(site) != SiteState.NOT_FOUND;
   }

   @ManagedOperation(description = "Brings the given site back online on all the cluster.", displayName = "Brings the given site back online on all the cluster.")
   public String bringSiteOnline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      BringSiteOnlineResponse rsp = takeOfflineManager.bringSiteOnline(site);
      if (rsp == BringSiteOnlineResponse.NO_SUCH_SITE) {
         return incorrectSiteName(site);
      }

      CacheRpcCommand command = commandsFactory.buildXSiteBringOnlineCommand(site);
      XSiteResponse<Void> response = invokeOnAll(command, new VoidResponseCollector(clusterSize()));

      return returnFailureOrSuccess(response.getErrors(), "Could not take the site online on nodes:");
   }

   @ManagedOperation(displayName = "Push state to site",
         description = "Pushes the state of this cache to the remote site. " +
                       "The remote site will be bring back online",
         name = "pushState")
   public final String pushState(@Parameter(description = "The destination site name", name = "SiteName") String siteName) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
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
    * for debug only!
    */
   public final List<String> getRunningStateTransfer() {
      return stateTransferManager.getRunningStateTransfers();
   }

   @ManagedOperation(displayName = "Push State Status",
         description = "Shows a map with destination site name and the state transfer status.",
         name = "PushStateStatus")
   public final Map<String, String> getPushStateStatus() {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
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
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      return performOperation("clearPushStateStatus", "(local)", () -> stateTransferManager.clearClusterStatus());
   }

   @ManagedOperation(displayName = "Cancel Push State",
         description = "Cancels the push state to remote site.",
         name = "CancelPushState")
   public final String cancelPushState(@Parameter(description = "The destination site name", name = "SiteName")
                                          final String siteName) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      return performOperation("cancelPushState", siteName, () -> stateTransferManager.cancelPushState(siteName));
   }

   @ManagedOperation(displayName = "Cancel Receive State",
         description = "Cancels the push state to this site. All the state received from state transfer " +
                       "will be ignored.",
         name = "CancelReceiveState")
   public final String cancelReceiveState(@Parameter(description = "The sending site name", name = "SiteName")
                                             final String siteName) {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      return performOperation("cancelReceiveState", siteName, () -> stateTransferManager.cancelReceive(siteName));
   }

   @ManagedOperation(displayName = "Sending Site Name",
         description = "Returns the site name from which this site is receiving state.",
         name = "SendingSiteName")
   public final String getSendingSiteName() {
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      return stateTransferManager.getSendingSiteName();
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
      authorizationHelper.checkPermission(AuthorizationPermission.ADMIN);
      if (takeOfflineManager.getSiteState(site) == SiteState.NOT_FOUND) {
         return incorrectSiteName(site);
      }

      CacheRpcCommand command = commandsFactory.buildXSiteAmendOfflineStatusCommand(site, afterFailures, minTimeToWait);
      XSiteResponse<Void> response = invokeOnAll(command, new VoidResponseCollector(clusterSize()));

      //also amend locally
      takeOfflineManager.amendConfiguration(site, afterFailures, minTimeToWait);

      return returnFailureOrSuccess(response.getErrors(), "Could not amend for nodes:");
   }

   private String returnFailureOrSuccess(List<Address> failed, String prefix) {
      if (!failed.isEmpty()) {
         return rpcError(failed, prefix);
      }
      return SUCCESS;
   }

   private String rpcError(List<Address> failed, String prefix) {
      return prefix + failed.toString();
   }

   private String incorrectSiteName(String site) {
      return "Incorrect site name: " + site;
   }

   private <T> XSiteResponse<T> invokeOnAll(CacheRpcCommand command, BaseResponseCollector<T> responseCollector) {
      RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
      CompletionStage<XSiteResponse<T>> rsp = rpcManager.invokeCommandOnAll(command, responseCollector, rpcOptions);
      return rpcManager.blocking(rsp);
   }

   private int clusterSize() {
      return rpcManager.getTransport().getMembers().size();
   }

   private interface Operation {
      void execute() throws Throwable;
   }

   private static abstract class BaseResponseCollector<T> extends ValidResponseCollector<XSiteResponse<T>> {

      final Map<Address, T> okResponses;
      final List<Address> errors;

      private BaseResponseCollector(int expectedSize) {
         okResponses = new HashMap<>(expectedSize);
         errors = new ArrayList<>(expectedSize);
      }

      @Override
      public final XSiteResponse<T> finish() {
         return new XSiteResponse<>(okResponses, errors);
      }

      abstract void storeResponse(Address sender, ValidResponse response);

      @Override
      protected final XSiteResponse<T> addValidResponse(Address sender, ValidResponse response) {
         storeResponse(sender, response);
         return null;
      }

      @Override
      protected final XSiteResponse<T> addTargetNotFound(Address sender) {
         //ignore leavers
         return null;
      }

      @Override
      protected final XSiteResponse<T> addException(Address sender, Exception exception) {
         errors.add(sender);
         return null;
      }
   }

   private static class XSiteResponse<T> {
      final Map<Address, T> responses;
      final List<Address> errors;

      private XSiteResponse(Map<Address, T> responses, List<Address> errors) {
         this.responses = responses;
         this.errors = errors;
      }

      boolean hasErrors() {
         return !errors.isEmpty();
      }

      List<Address> getErrors() {
         return errors;
      }

      void forEachError(Consumer<Address> consumer) {
         errors.forEach(consumer);
      }

      void forEach(BiConsumer<Address, T> consumer) {
         responses.forEach(consumer);
      }
   }

   private static class VoidResponseCollector extends BaseResponseCollector<Void> {

      private VoidResponseCollector(int expectedSize) {
         super(expectedSize);
      }

      @Override
      void storeResponse(Address sender, ValidResponse response) {
         //no-op
      }
   }

   private static class XSiteStatusResponseCollector extends BaseResponseCollector<Boolean> {

      private XSiteStatusResponseCollector(int expectedSize) {
         super(expectedSize);
      }

      @Override
      void storeResponse(Address sender, ValidResponse response) {
         Object value = response.getResponseValue();
         assert value instanceof Boolean;
         okResponses.put(sender, (Boolean) value);
      }
   }

   private static class PerSiteBooleanResponseCollector extends BaseResponseCollector<Map<String, Boolean>> {

      private PerSiteBooleanResponseCollector(int expectedSize) {
         super(expectedSize);
      }

      @Override
      void storeResponse(Address sender, ValidResponse response) {
         //noinspection unchecked
         okResponses.put(sender, (Map<String, Boolean>) response.getResponseValue());
      }
   }
}
