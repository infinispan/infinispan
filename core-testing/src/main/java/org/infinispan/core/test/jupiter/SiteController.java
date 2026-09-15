package org.infinispan.core.test.jupiter;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.xsite.XSiteAdminOperations;
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
   private final List<String> disconnectedSites = new ArrayList<>();

   SiteController(XSiteClusterHandle cluster) {
      this.cluster = cluster;
   }

   /**
    * Disconnects a site at the network level by inserting DISCARD on the
    * RELAY2 bridge of every site master in every <em>other</em> site.
    * <p>
    * After this call, the target site is still running but unreachable from
    * all other sites. Cross-site replication requests to this site will
    * time out or fail.
    *
    * @param site the site name to disconnect
    */
   public void disconnect(String site) {
      // Get the JGroups addresses of site masters in the target site
      List<org.jgroups.Address> targetAddresses = siteAddresses(site);

      // On every other site's nodes, discard messages from/to the target site
      for (var entry : cluster.sites().entrySet()) {
         if (entry.getKey().equals(site)) continue;
         for (EmbeddedCacheManager manager : entry.getValue().managers()) {
            RELAY2 relay = findRelay(manager);
            if (relay == null || !relay.isSiteMaster()) continue;
            JChannel bridgeChannel = relay.getBridge(site);
            if (bridgeChannel == null) continue;

            DISCARD discard = findDiscard(bridgeChannel);
            if (discard == null) {
               discard = new DISCARD();
               discard.discardAll(true);
               try {
                  bridgeChannel.getProtocolStack().insertProtocol(
                        discard, ProtocolStack.Position.ABOVE, TP.class);
               } catch (Exception e) {
                  throw new RuntimeException("Failed to insert DISCARD on bridge channel", e);
               }
            } else {
               discard.discardAll(true);
            }
         }
      }

      // Also discard on the target site's own bridge channels to other sites
      for (EmbeddedCacheManager manager : cluster.site(site).managers()) {
         RELAY2 relay = findRelay(manager);
         if (relay == null || !relay.isSiteMaster()) continue;
         for (var otherSite : cluster.sites().keySet()) {
            if (otherSite.equals(site)) continue;
            JChannel bridgeChannel = relay.getBridge(otherSite);
            if (bridgeChannel == null) continue;

            DISCARD discard = findDiscard(bridgeChannel);
            if (discard == null) {
               discard = new DISCARD();
               discard.discardAll(true);
               try {
                  bridgeChannel.getProtocolStack().insertProtocol(
                        discard, ProtocolStack.Position.ABOVE, TP.class);
               } catch (Exception e) {
                  throw new RuntimeException("Failed to insert DISCARD on bridge channel", e);
               }
            } else {
               discard.discardAll(true);
            }
         }
      }

      disconnectedSites.add(site);
   }

   /**
    * Reconnects a previously disconnected site by removing DISCARD from
    * all RELAY2 bridge channels.
    *
    * @param site the site name to reconnect
    */
   public void reconnect(String site) {
      // Remove DISCARD from other sites' bridges to the target site
      for (var entry : cluster.sites().entrySet()) {
         if (entry.getKey().equals(site)) continue;
         for (EmbeddedCacheManager manager : entry.getValue().managers()) {
            RELAY2 relay = findRelay(manager);
            if (relay == null || !relay.isSiteMaster()) continue;
            JChannel bridgeChannel = relay.getBridge(site);
            if (bridgeChannel != null) {
               removeDISCARD(bridgeChannel);
            }
         }
      }

      // Remove DISCARD from the target site's bridges to other sites
      for (EmbeddedCacheManager manager : cluster.site(site).managers()) {
         RELAY2 relay = findRelay(manager);
         if (relay == null || !relay.isSiteMaster()) continue;
         for (var otherSite : cluster.sites().keySet()) {
            if (otherSite.equals(site)) continue;
            JChannel bridgeChannel = relay.getBridge(otherSite);
            if (bridgeChannel != null) {
               removeDISCARD(bridgeChannel);
            }
         }
      }

      disconnectedSites.remove(site);
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
      for (String site : new ArrayList<>(disconnectedSites)) {
         reconnect(site);
      }
   }

   private List<org.jgroups.Address> siteAddresses(String site) {
      List<org.jgroups.Address> addresses = new ArrayList<>();
      for (EmbeddedCacheManager manager : cluster.site(site).managers()) {
         addresses.add(NetworkController.extractChannel(manager).getAddress());
      }
      return addresses;
   }

   private static RELAY2 findRelay(EmbeddedCacheManager manager) {
      JChannel channel = NetworkController.extractChannel(manager);
      return channel.getProtocolStack().findProtocol(RELAY2.class);
   }

   private static DISCARD findDiscard(JChannel channel) {
      return channel.getProtocolStack().findProtocol(DISCARD.class);
   }

   private static void removeDISCARD(JChannel channel) {
      if (findDiscard(channel) != null) {
         channel.getProtocolStack().removeProtocol(DISCARD.class);
      }
   }
}
