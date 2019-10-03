package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.infinispan.xsite.status.CacheSiteStatusBuilder;
import org.infinispan.xsite.status.SiteStatus;

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
   private static Log log = LogFactory.getLog(XSiteAdminOperations.class);

   @Inject RpcManager rpcManager;
   @Inject Cache cache;
   @Inject volatile BackupSender backupSender;
   @Inject XSiteStateTransferManager stateTransferManager;

   public Map<String, SiteStatus> clusterStatus()  {
      Map<String, Boolean> localNodeStatus = backupSender.status();
      XSiteAdminCommand command = new XSiteAdminCommand(ByteString.fromString(cache.getName()), null, XSiteAdminCommand.AdminOperation.STATUS, null, null);
      Map<Address, Response> responses = invokeRemotely(command);
      List<Address> errors = checkForErrors(responses);
      if (!errors.isEmpty()) {
         throw new CacheException("Unable to check cluster state for members: " + errors);
      }
      //site name => online/offline/mixed
      Map<String, CacheSiteStatusBuilder> perSiteBuilder = new HashMap<>();
      for (Map.Entry<String, Boolean> entry : localNodeStatus.entrySet()) {
         CacheSiteStatusBuilder builder = new CacheSiteStatusBuilder();
         builder.addMember(rpcManager.getAddress(), entry.getValue());
         perSiteBuilder.put(entry.getKey(), builder);
      }
      for (Map.Entry<Address, Response> entry : responses.entrySet()) {
         Response response = entry.getValue();
         if (response == CacheNotFoundResponse.INSTANCE) {
            continue; //shutting down.
         }
         if (!response.isSuccessful()) {
            throw new CacheException("Unsuccessful response received from. " + entry);
         }
         //noinspection unchecked
         Map<String, Boolean> sites = (Map<String, Boolean>) ((SuccessfulResponse) response).getResponseValue();
         for (Map.Entry<String, Boolean> site : sites.entrySet()) {
            CacheSiteStatusBuilder builder = perSiteBuilder.get(site.getKey());
            if (builder == null) {
               throw new IllegalStateException("Site " + entry.getKey() + " not defined in all the cluster members");
            }
            builder.addMember(entry.getKey(), site.getValue());
         }
      }

      Map<String, SiteStatus> result = new HashMap<>();
      perSiteBuilder.forEach((site, builder) -> result.put(site, builder.build()));
      return result;
   }

   @ManagedOperation(description = "Check whether the given backup site is offline or not.", displayName = "Check whether the given backup site is offline or not.")
   public String siteStatus(@Parameter(name = "site", description = "The name of the backup site") String site) {
      //also consider local node
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return "Incorrect site name: " + site;
      log.tracef("This node's status is %s", offlineStatus);

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
      //also consider local node
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         throw new IllegalArgumentException("Incorrect site name: " + site);
      log.tracef("This node's status is %s", offlineStatus);
      XSiteAdminCommand command = new XSiteAdminCommand(ByteString.fromString(cache.getName()), site, XSiteAdminCommand.AdminOperation.SITE_STATUS, null, null);
      Map<Address, Response> responses = invokeRemotely(command);
      Map<Address, String> statusMap = new HashMap<>();
      for (Map.Entry<Address, Response> e : responses.entrySet()) {
         if (!e.getValue().isSuccessful() || !e.getValue().isValid()) {
            if (e.getValue() != CacheNotFoundResponse.INSTANCE) {
               //the node can be shutting down.
               statusMap.put(e.getKey(), FAILED);
            }
            continue;
         }
         SuccessfulResponse response = (SuccessfulResponse) e.getValue();
         log.tracef("Got status %s from node %s", response.getResponseValue(), e.getKey());
         if (response.getResponseValue() == XSiteAdminCommand.Status.OFFLINE) {
            statusMap.put(e.getKey(), OFFLINE);
         } else if (response.getResponseValue() == XSiteAdminCommand.Status.ONLINE) {
            statusMap.put(e.getKey(), ONLINE);
         } else {
            throw new IllegalStateException("Unknown response: " + response.getResponseValue());
         }
      }

      if (offlineStatus.isOffline()) {
         statusMap.put(rpcManager.getAddress(), OFFLINE);
      } else {
         statusMap.put(rpcManager.getAddress(), ONLINE);
      }

      return statusMap;
   }

   /**
    * Returns a Map&lt;String,String&gt; with each site and the status
    */
   public Map<String, String> siteStatuses() throws CacheException {
      Map<String, Boolean> localNodeStatus = backupSender.status();
      XSiteAdminCommand command = new XSiteAdminCommand(ByteString.fromString(cache.getName()), null, XSiteAdminCommand.AdminOperation.STATUS, null, null);
      Map<Address, Response> responses = invokeRemotely(command);
      List<Address> errors = checkForErrors(responses);
      if (!errors.isEmpty()) throw new CacheException("Failure invoking 'status()' on nodes: " + errors);

      //<site name, nodes where it failed>
      Map<String, List<Address>> result = new HashMap<>();
      for (Entry<String, Boolean> e : localNodeStatus.entrySet()) {
         ArrayList<Address> failedSites = new ArrayList<>();
         result.put(e.getKey(), failedSites);
         if (!e.getValue()) {
            failedSites.add(rpcManager.getAddress());
         }
      }
      for (Entry<Address, Response> response : responses.entrySet()) {
         @SuppressWarnings("unchecked")
         Map<String, Boolean> status = (Map<String, Boolean>) ((SuccessfulResponse) response.getValue()).getResponseValue();
         for (Entry<String, Boolean> entry : status.entrySet()) {
            List<Address> addresses = result.get(entry.getKey());
            if (addresses == null)
               throw new IllegalStateException("All sites must be defined on all the nodes of the cluster!");
            if (!entry.getValue()) {
               addresses.add(rpcManager.getAddress());
            }
         }
      }

      int clusterSize = rpcManager.getTransport().getMembers().size();
      return result.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
               List<Address> value = e.getValue();
               if (value.isEmpty()) return XSiteAdminOperations.ONLINE;
               if (value.size() == clusterSize) return XSiteAdminOperations.OFFLINE;
               return "mixed, offline on nodes: " + value;
            }
      ));
   }

   @ManagedOperation(description = "Returns the the status(offline/online) of all the configured backup sites.", displayName = "Returns the the status(offline/online) of all the configured backup sites.")
   public String status() {
      Map<String, String> statuses = siteStatuses();

      StringBuilder resultStr = new StringBuilder();
      //now generate the final response
      boolean first = true;
      for (Entry<String, String> e : statuses.entrySet()) {
         if (!first) {
            resultStr.append("\n");
         } else first = false;
         resultStr.append(e.getKey()).append("[");
         String value = e.getValue();
         resultStr.append(value.toUpperCase());
         resultStr.append("]");
      }
      return resultStr.toString();
   }

   @ManagedOperation(description = "Takes this site offline in all nodes in the cluster.", displayName = "Takes this site offline in all nodes in the cluster.")
   public String takeSiteOffline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return incorrectSiteName(site);
      backupSender.takeSiteOffline(site);
      log.tracef("Is site offline in node %s? %s", rpcManager.getAddress(), offlineStatus.isOffline());

      XSiteAdminCommand command = new XSiteAdminCommand(ByteString.fromString(cache.getName()), site, XSiteAdminCommand.AdminOperation.TAKE_OFFLINE, null, null);
      Map<Address, Response> responses = invokeRemotely(command);

      List<Address> failed = checkForErrors(responses);

      String prefix = "Could not take the site offline on nodes:";
      return returnFailureOrSuccess(failed, prefix);
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
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null) return incorrectSiteName(site);
      return String.valueOf(offlineStatus.getTakeOffline().minTimeToWait());
   }

   @ManagedOperation(description = "Returns the value of the 'afterFailures' for the 'TakeOffline' functionality.", displayName = "Returns the value of the 'afterFailures' for the 'TakeOffline' functionality.")
   public String getTakeOfflineAfterFailures(@Parameter(name = "site", description = "The name of the backup site") String site) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null) return incorrectSiteName(site);
      return String.valueOf(offlineStatus.getTakeOffline().afterFailures());
   }

   public OfflineStatus getOfflineStatus(String site) {
      return backupSender.getOfflineStatus(site);
   }

   public boolean checkSite(String site) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      return offlineStatus != null;
   }

   @ManagedOperation(description = "Brings the given site back online on all the cluster.", displayName = "Brings the given site back online on all the cluster.")
   public String bringSiteOnline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return "Incorrect site name: " + site;
      backupSender.bringSiteOnline(site);

      XSiteAdminCommand command = new XSiteAdminCommand(ByteString.fromString(cache.getName()), site, XSiteAdminCommand.AdminOperation.BRING_ONLINE, null, null);
      Map<Address, Response> responses = invokeRemotely(command);

      List<Address> failed = checkForErrors(responses);

      return returnFailureOrSuccess(failed, "Could not take the site online on nodes:");
   }

   @ManagedOperation(displayName = "Push state to site",
                     description = "Pushes the state of this cache to the remote site. " +
                           "The remote site will be bring back online",
                     name = "pushState")
   public final String pushState(@Parameter(description = "The destination site name", name = "SiteName") String siteName) {
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
      Map<String, String> map = new HashMap<>();
      try {
         for (String siteName : getRunningStateTransfer()) {
            map.put(siteName, XSiteStateTransferManager.STATUS_SENDING);
         }
         map.putAll(stateTransferManager.getClusterStatus());
         return map;
      } catch (Exception e) {
         return Collections.singletonMap(XSiteStateTransferManager.STATUS_ERROR, e.getLocalizedMessage());
      }
   }

   @ManagedOperation(displayName = "Clear State Status",
                     description = "Clears the state transfer status.",
                     name = "ClearPushStateStatus")
   public final String clearPushStateStatus() {
      return performOperation("clearPushStateStatus", "(local)", () -> stateTransferManager.clearClusterStatus());
   }

   @ManagedOperation(displayName = "Cancel Push Status",
                     description = "Cancels the push state to remote site.",
                     name = "CancelPushState")
   public final String cancelPushState(@Parameter(description = "The destination site name", name = "SiteName")
                                          final String siteName) {
      return performOperation("cancelPushState", siteName, () -> stateTransferManager.cancelPushState(siteName));
   }

   @ManagedOperation(displayName = "Cancel Receive State",
                     description = "Cancels the push state to this site. All the state received from state transfer " +
                           "will be ignored.",
                     name = "CancelReceiveState")
   public final String cancelReceiveState(@Parameter(description = "The sending site name", name = "SiteName")
                                             final String siteName) {
      return performOperation("cancelReceiveState", siteName, () -> stateTransferManager.cancelReceive(siteName));
   }

   @ManagedOperation(displayName = "Sending Site Name",
                     description = "Returns the site name from which this site is receiving state.",
                     name = "SendingSiteName")
   public final String getSendingSiteName() {
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

   private List<Address> checkForErrors(Map<Address, Response> responses) {
      List<Address> failed = new ArrayList<>(responses.size());
      for (Map.Entry<Address, Response> e : responses.entrySet()) {
         if (e.getValue() != CacheNotFoundResponse.INSTANCE &&
               (e.getValue() == null || !e.getValue().isSuccessful() || !e.getValue().isValid())) {
            failed.add(e.getKey());
         }
      }
      return failed;
   }

   private String takeOffline(String site, Integer afterFailures, Long minTimeToWait) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return incorrectSiteName(site);

      XSiteAdminCommand command = new XSiteAdminCommand(ByteString.fromString(cache.getName()), site, XSiteAdminCommand.AdminOperation.AMEND_TAKE_OFFLINE,
                                                        afterFailures, minTimeToWait);
      Map<Address, Response> responses = invokeRemotely(command);

      //also amend locally
      offlineStatus.amend(afterFailures, minTimeToWait);

      List<Address> failed = checkForErrors(responses);

      return returnFailureOrSuccess(failed, "Could not amend for nodes:");
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

   private Map<Address, Response> invokeRemotely(XSiteAdminCommand command) {
      return rpcManager.invokeRemotely(null, command,
                                       rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build());
   }

   private interface Operation {
      void execute() throws Throwable;
   }
}
