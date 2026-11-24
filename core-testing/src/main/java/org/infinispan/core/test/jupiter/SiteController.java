package org.infinispan.core.test.jupiter;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.xsite.XSiteAdminOperations;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.stack.ProtocolStack;

/**
 * Controls cross-site availability for failure injection in xsite tests.
 * <p>
 * Provides capabilities to:
 * <ul>
 *   <li>Disconnect a site at the network level (RELAY2 bridge DISCARD)</li>
 *   <li>Take a backup site offline at the application level (per cache)</li>
 *   <li>Bring sites back online and restore connectivity</li>
 * </ul>
 * <p>
 * Obtained via {@link XSiteContext#sites()}.
 *
 * <h3>Example: Network-level site disconnection</h3>
 * <pre>{@code
 * ctx.sites().disconnect("NYC");       // NYC becomes unreachable
 * // ... test failure scenario ...
 * ctx.sites().reconnect("NYC");        // restore connectivity
 * }</pre>
 *
 * <h3>Example: Application-level site offline</h3>
 * <pre>{@code
 * ctx.sites().takeSiteOffline(cache, "NYC");   // stop replicating to NYC
 * // ... test offline behavior ...
 * ctx.sites().bringSiteOnline(cache, "NYC");   // resume replication
 * }</pre>
 *
 * @since 16.2
 */
public class SiteController {

   private final XSiteClusterHandle cluster;
   private final Set<String> disconnectedSites = new LinkedHashSet<>();

   SiteController(XSiteClusterHandle cluster) {
      this.cluster = cluster;
   }

   /**
    * Disconnects a site at the network level, leaving all other sites able to
    * communicate with each other.
    * <p>
    * All site masters share a single bridge cluster, so a blanket discard on a
    * bridge channel would also block traffic between unrelated sites. Instead
    * a {@link DISCARD} filter is installed on every bridge channel and
    * configured to drop only the messages exchanged with the site masters of
    * the disconnected sites.
    * <p>
    * After this call, the target site is still running but unreachable from
    * all other sites. Cross-site replication requests to this site will
    * time out or fail.
    *
    * @param site the site name to disconnect
    */
   public void disconnect(String site) {
      cluster.site(site); // fail fast on unknown sites
      disconnectedSites.add(site);
      applyBridgeFilters();
   }

   /**
    * Reconnects a previously disconnected site.
    * <p>
    * Disconnections requested independently for other sites are preserved.
    *
    * @param site the site name to reconnect
    */
   public void reconnect(String site) {
      cluster.site(site); // fail fast on unknown sites
      disconnectedSites.remove(site);
      applyBridgeFilters();
   }

   /**
    * Takes a backup site offline for a specific cache using the
    * {@link XSiteAdminOperations} API.
    * <p>
    * This is an application-level operation — the site is still reachable
    * but replication to it is suspended for the given cache.
    *
    * @param cache      the cache to stop replicating from
    * @param targetSite the backup site to take offline
    */
   public void takeSiteOffline(Cache<?, ?> cache, String targetSite) {
      XSiteAdminOperations admin = ComponentRegistry.componentOf(cache, XSiteAdminOperations.class);
      admin.takeSiteOffline(targetSite);
   }

   /**
    * Brings a backup site back online for a specific cache.
    *
    * @param cache      the cache to resume replication from
    * @param targetSite the backup site to bring online
    */
   public void bringSiteOnline(Cache<?, ?> cache, String targetSite) {
      XSiteAdminOperations admin = ComponentRegistry.componentOf(cache, XSiteAdminOperations.class);
      admin.bringSiteOnline(targetSite);
   }

   /**
    * Returns the site status (online/offline) for a backup site as seen by a cache.
    *
    * @param cache      the cache to check
    * @param targetSite the backup site
    * @return the status string (e.g. "online", "offline")
    */
   public String siteStatus(Cache<?, ?> cache, String targetSite) {
      XSiteAdminOperations admin = ComponentRegistry.componentOf(cache, XSiteAdminOperations.class);
      return admin.siteStatus(targetSite);
   }

   void reset() {
      disconnectedSites.clear();
      applyBridgeFilters();
   }

   /**
    * Recomputes the {@link DISCARD} filters on every site master's bridge
    * channel from the current set of disconnected sites, so that only the
    * traffic between two sites separated by a disconnection is dropped.
    * <p>
    * The bridge channel of site {@code X} drops messages from site {@code Y}
    * when {@code X != Y} and either {@code X} or {@code Y} is disconnected.
    */
   private void applyBridgeFilters() {
      for (String siteName : cluster.sites().keySet()) {
         // Site masters of the sites separated from siteName by a disconnection
         List<Address> ignoredSenders = new ArrayList<>();
         for (String otherSite : cluster.sites().keySet()) {
            if (otherSite.equals(siteName)) continue;
            if (disconnectedSites.contains(siteName) || disconnectedSites.contains(otherSite)) {
               ignoredSenders.addAll(siteMasterBridgeAddresses(otherSite));
            }
         }

         for (EmbeddedCacheManager manager : cluster.site(siteName).managers()) {
            JChannel bridge = bridgeChannel(siteName, manager);
            if (bridge == null) continue;
            ignoreSenders(bridge, ignoredSenders);
         }
      }
   }

   /**
    * Returns the bridge channel addresses of every site master in the given site.
    */
   private List<Address> siteMasterBridgeAddresses(String site) {
      List<Address> addresses = new ArrayList<>();
      for (EmbeddedCacheManager manager : cluster.site(site).managers()) {
         JChannel bridge = bridgeChannel(site, manager);
         if (bridge != null) {
            addresses.add(bridge.getAddress());
         }
      }
      return addresses;
   }

   /**
    * Returns the shared bridge channel of a site master, or {@code null} if the
    * manager is not a site master or its bridge has not been started yet.
    */
   private JChannel bridgeChannel(String managerSite, EmbeddedCacheManager manager) {
      RELAY2 relay = findRelay(manager);
      if (relay == null || !relay.isSiteMaster()) {
         return null;
      }
      // Every site master runs a single bridge channel shared by all routes,
      // so any other site's route gives access to it
      return cluster.sites().keySet().stream()
            .filter(s -> !s.equals(managerSite))
            .map(relay::getBridge)
            .filter(bridge -> bridge != null)
            .findFirst()
            .orElse(null);
   }

   /**
    * Drops messages received from the given senders on the bridge channel,
    * inserting or reconfiguring a {@link DISCARD} protocol as necessary.
    * An empty sender list lets everything through.
    */
   private static void ignoreSenders(JChannel bridge, List<Address> senders) {
      DISCARD discard = findDiscard(bridge);
      if (senders.isEmpty()) {
         // Nothing to filter: remove a leftover DISCARD, if any
         if (discard != null) {
            bridge.getProtocolStack().removeProtocol(discard);
         }
         return;
      }
      if (discard == null) {
         discard = new DISCARD();
         try {
            bridge.getProtocolStack().insertProtocol(
                  discard, ProtocolStack.Position.ABOVE, TP.class);
         } catch (Exception e) {
            throw new RuntimeException("Failed to insert DISCARD on bridge channel", e);
         }
      }
      discard.resetIgnoredMembers();
      discard.addIgnoredMembers(senders.toArray(new Address[0]));
   }

   private static RELAY2 findRelay(EmbeddedCacheManager manager) {
      JChannel channel = NetworkController.extractChannel(manager);
      return channel.getProtocolStack().findProtocol(RELAY2.class);
   }

   private static DISCARD findDiscard(JChannel channel) {
      return channel.getProtocolStack().findProtocol(DISCARD.class);
   }
}
