/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Managed bean exposing sys admin operations for Cross-Site replication functionality.
 *
 * @author Mircea Markus
 * @since 5.2
 */
@MBean(objectName = "XSiteAdmin", description = "Exposes tooling for handling backing up data to remote sites.")
public class XSiteAdminOperations {

   private static Log log = LogFactory.getLog(XSiteAdminOperations.class);

   public static final String ONLINE = "online";
   public static final String OFFLINE = "offline";
   public static final String SUCCESS = "ok";

   private RpcManager rpcManager;
   private Cache cache;

   private volatile BackupSender backupSender;

   @Inject
   public void init(RpcManager rpcManager, BackupSender backupSender, Cache cache) {
      this.backupSender = backupSender;
      this.rpcManager = rpcManager;
      this.backupSender = backupSender;
      this.cache = cache;
   }

   @ManagedOperation(description = "Check whether the given backup site is offline or not.", displayName = "Check whether the given backup site is offline or not.")
   public String siteStatus(@Parameter(name = "site", description = "The name of the backup site") String site) {
      //also consider local node
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return "Incorrect site name: " + offlineStatus;
      log.tracef("This node's status is %s", offlineStatus);

      XSiteAdminCommand command = new XSiteAdminCommand(cache.getName(), site, XSiteAdminCommand.AdminOperation.SITE_STATUS, null, null);
      Map<Address, Response> responses = invokeRemotely(command);
      List<Address> online = new ArrayList<Address>(responses.size());
      List<Address> offline = new ArrayList<Address>(responses.size());
      List<Address> failed = new ArrayList<Address>(responses.size());
      for (Map.Entry<Address, Response> e : responses.entrySet()) {
         if (!e.getValue().isSuccessful() || !e.getValue().isValid()) {
            failed.add(e.getKey());
            continue;
         }
         SuccessfulResponse response = (SuccessfulResponse) e.getValue();
         log.tracef("Got status %s from node %s", response.getResponseValue(), e.getKey());
         if (response.getResponseValue() == XSiteAdminCommand.Status.OFFLINE) {
            offline.add(e.getKey());
         } else if (response.getResponseValue() == XSiteAdminCommand.Status.ONLINE) {
            online.add(e.getKey());
         } else {
            throw new IllegalStateException("Unknown response: " + response.getResponseValue());
         }
      }
      if (!failed.isEmpty()) {
         return rpcError(failed, "Could not query nodes ");
      }

      if (offlineStatus.isOffline()) {
         offline.add(rpcManager.getAddress());
      } else {
         online.add(rpcManager.getAddress());
      }

      if (offline.isEmpty()) {
         return ONLINE;
      }
      if (online.isEmpty()) {
         return OFFLINE;
      }
      return "Site appears online on nodes:" + online + " and offline on nodes: " + offline;
   }

   @ManagedOperation(description = "Returns the the status(offline/online) of all the configured backup sites.", displayName = "Returns the the status(offline/online) of all the configured backup sites.")
   public String status() {
      //also consider local node
      Map<String, Boolean> localNodeStatus = backupSender.status();
      XSiteAdminCommand command = new XSiteAdminCommand(cache.getName(), null, XSiteAdminCommand.AdminOperation.STATUS, null, null);
      Map<Address, Response> responses = invokeRemotely(command);
      List<Address> errors = checkForErrors(responses);
      if (!errors.isEmpty()) {
         return rpcError(errors, "Failure invoking 'status()' on nodes: ");
      }
      //<site name, nodes where it failed>
      Map<String, List<Address>> result = new HashMap<String, List<Address>>();
      for (Map.Entry<String, Boolean> e : localNodeStatus.entrySet()) {
         ArrayList<Address> failedSites = new ArrayList<Address>();
         result.put(e.getKey(), failedSites);
         if (!e.getValue()) {
            failedSites.add(rpcManager.getAddress());
         }
      }
      for (Map.Entry<Address, Response> response : responses.entrySet()) {
         Map<String, Boolean> status = (Map<String, Boolean>) ((SuccessfulResponse)response.getValue()).getResponseValue();
         for (Map.Entry<String, Boolean> entry : status.entrySet()) {
            List<Address> addresses = result.get(entry.getKey());
            if (addresses == null)
               throw new IllegalStateException("All sites must be defined on all the nodes of the cluster!");
            if (!entry.getValue()) {
               addresses.add(rpcManager.getAddress());
            }
         }
      }

      int clusterSize = rpcManager.getTransport().getMembers().size();

      StringBuilder resultStr = new StringBuilder();
      //now generate the final response
      boolean first = true;
      for (Map.Entry<String, List<Address>> e : result.entrySet()) {
         if (!first) {
            resultStr.append("\n");
         } else first = false;
         resultStr.append(e.getKey()).append("[");
         List<Address> value = e.getValue();
         if (value.isEmpty()) {
            resultStr.append("ONLINE");
         } else if (value.size() == clusterSize) {
            resultStr.append("OFFLINE");
         } else {
            resultStr.append("MIXED, offline on nodes: ").append(value);
         }
         resultStr.append("]");
      }
      return resultStr.toString();
   }

   @ManagedOperation(description = "Takes this site offline in all nodes in the cluster.",displayName = "Takes this site offline in all nodes in the cluster.")
   public String takeSiteOffline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return incorrectSiteName(site);
      backupSender.takeSiteOffline(site);
      log.tracef("Is site offline in node %s? %s", rpcManager.getAddress(), offlineStatus.isOffline());

      XSiteAdminCommand command = new XSiteAdminCommand(cache.getName(), site, XSiteAdminCommand.AdminOperation.TAKE_OFFLINE, null, null);
      Map<Address, Response> responses = invokeRemotely(command);

      List<Address> failed = checkForErrors(responses);

      String prefix = "Could not take the site offline on nodes:";
      return returnFailureOrSuccess(failed, prefix);
   }

   @ManagedOperation(description = "Amends the values for 'afterFailures' for the 'TakeOffline' functionality on all the nodes in the cluster.", displayName = "Amends the values for 'TakeOffline.afterFailures' on all the nodes in the cluster.")
   public String setTakeOfflineAfterFailures(
         @Parameter(name = "site", description = "The name of the backup site") String site,
         @Parameter(name = "afterFailures", description = "The number of failures after which the site will be taken offline",
                    type = "integer") int afterFailures) {
      return takeOffline(site, afterFailures, null);
   }

   @ManagedOperation(description = "Amends the values for 'minTimeToWait' for the 'TakeOffline' functionality on all the nodes in the cluster.", displayName = "Amends the values for 'TakeOffline.minTimeToWait' on all the nodes in the cluster.")
   public String setTakeOfflineMinTimeToWait(
         @Parameter(name = "site", description = "The name of the backup site") String site,
         @Parameter(name = "minTimeToWait", description = "The minimum amount of time in milliseconds to wait before taking a site offline", type = "long") long minTimeToWait) {
      return takeOffline(site, null,  minTimeToWait);
   }

   @ManagedOperation(description = "Amends the values for 'TakeOffline' functionality on all the nodes in the cluster.", displayName = "Amends the values for 'TakeOffline' functionality on all the nodes in the cluster.")
   public String amendTakeOffline(
         @Parameter(name = "site", description = "The name of the backup site") String site,
         @Parameter(name = "afterFailures", description = "The number of failures after which the site will be taken offline", type = "integer") int afterFailures,
         @Parameter(name = "minTimeToWait", description = "The minimum amount of time in milliseconds to wait before taking a site offline", type = "long") long minTimeToWait) {
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

   @ManagedOperation(description = "Brings the given site back online on all the cluster.", displayName = "Brings the given site back online on all the cluster.")
   public String bringSiteOnline(@Parameter(name = "site", description = "The name of the backup site") String site) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return "Incorrect site name: " + offlineStatus;
      backupSender.bringSiteOnline(site);

      XSiteAdminCommand command = new XSiteAdminCommand(cache.getName(), site, XSiteAdminCommand.AdminOperation.BRING_ONLINE, null, null);
      Map<Address, Response> responses = invokeRemotely(command);

      List<Address> failed = checkForErrors(responses);

      return returnFailureOrSuccess(failed, "Could not take the site online on nodes:");
   }

   private List<Address> checkForErrors(Map<Address, Response> responses) {
      List<Address> failed = new ArrayList<Address>(responses.size());
      for (Map.Entry<Address, Response> e : responses.entrySet()) {
         if (e.getValue() == null || !e.getValue().isSuccessful() || !e.getValue().isValid()) {
            failed.add(e.getKey());
         }
      }
      return failed;
   }

   private String takeOffline(String site, Integer afterFailures, Long minTimeToWait) {
      OfflineStatus offlineStatus = backupSender.getOfflineStatus(site);
      if (offlineStatus == null)
         return incorrectSiteName(site);

      XSiteAdminCommand command = new XSiteAdminCommand(cache.getName(), site, XSiteAdminCommand.AdminOperation.AMEND_TAKE_OFFLINE,
                                                        afterFailures, minTimeToWait);
      Map<Address, Response> responses = invokeRemotely(command);

      //also amend locally
      offlineStatus.amend(afterFailures,minTimeToWait);

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
      return rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(true, false));
   }
}
