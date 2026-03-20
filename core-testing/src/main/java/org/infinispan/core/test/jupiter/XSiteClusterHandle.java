package org.infinispan.core.test.jupiter;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.core.test.jupiter.transport.TestRelay;
import org.infinispan.core.test.jupiter.transport.TestTransport;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Transport;

/**
 * Manages a cross-site topology of multiple independent clusters connected via RELAY2.
 * <p>
 * Created once per test class; each site is its own cluster with
 * RELAY2 bridges enabling cross-site replication.
 *
 * @since 16.2
 */
class XSiteClusterHandle implements AutoCloseable {

   private final Map<String, SiteInfo> sites;
   private final ControlledTimeService timeService;
   private final boolean globalState;
   private final Path stateBaseDir;

   XSiteClusterHandle(Site[] siteAnnotations, boolean controlledTime,
                      List<SerializationContextInitializer> contextInitializers, boolean globalState) {
      this.timeService = controlledTime ? new ControlledTimeService() : null;
      this.globalState = globalState;
      this.sites = new LinkedHashMap<>();

      if (globalState) {
         try {
            this.stateBaseDir = Files.createTempDirectory("ispn-xsite-state-");
         } catch (IOException e) {
            throw new UncheckedIOException("Failed to create state directory", e);
         }
      } else {
         this.stateBaseDir = null;
      }

      List<String> siteNames = new ArrayList<>();
      for (Site s : siteAnnotations) {
         siteNames.add(s.name());
      }

      String bridgeClusterName = "bridge-" + System.nanoTime();
      TestRelay.registerTopology(bridgeClusterName, siteNames);

      for (Site s : siteAnnotations) {
         String siteName = s.name();
         int numNodes = s.nodes();
         String clusterName = "ISPN(SITE " + siteName + ")-" + System.nanoTime();
         int portOffset = TestTransport.allocatePortOffset();

         List<EmbeddedCacheManager> managers = new ArrayList<>(numNodes);
         for (int i = 0; i < numNodes; i++) {
            ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
            GlobalConfigurationBuilder gcb = holder.getGlobalConfigurationBuilder();

            TestTransport.configureXSite(gcb, clusterName, siteName,
                  bridgeClusterName, i, portOffset);

            for (SerializationContextInitializer sci : contextInitializers) {
               gcb.serialization().addContextInitializer(sci);
            }

            if (globalState) {
               Path nodeDir = stateBaseDir.resolve(siteName + "-node-" + i);
               try {
                  Files.createDirectories(nodeDir);
               } catch (IOException e) {
                  throw new UncheckedIOException("Failed to create node state directory", e);
               }
               gcb.globalState().enable()
                     .persistentLocation(nodeDir.toString())
                     .sharedPersistentLocation(nodeDir.toString())
                     .temporaryLocation(nodeDir.resolve("tmp").toString());
            }

            EmbeddedCacheManager manager = new DefaultCacheManager(holder, true);

            if (timeService != null) {
               replaceTimeService(manager, timeService);
            }

            managers.add(manager);
         }

         sites.put(siteName, new SiteInfo(siteName, managers, numNodes));
      }

      // Wait for intra-site clustering
      for (SiteInfo info : sites.values()) {
         if (info.numNodes > 1) {
            waitForIntraSiteCluster(info);
         }
      }

      // Wait for cross-site bridge views
      waitForXSiteViews(siteNames);
   }

   Map<String, SiteInfo> sites() {
      return Collections.unmodifiableMap(sites);
   }

   SiteInfo site(String name) {
      SiteInfo info = sites.get(name);
      if (info == null) {
         throw new IllegalArgumentException("Unknown site: " + name + ". Available: " + sites.keySet());
      }
      return info;
   }

   ControlledTimeService timeService() {
      return timeService;
   }

   @Override
   public void close() {
      // Stop in reverse order
      List<SiteInfo> reversed = new ArrayList<>(sites.values());
      Collections.reverse(reversed);
      for (SiteInfo info : reversed) {
         for (int i = info.managers.size() - 1; i >= 0; i--) {
            try {
               info.managers.get(i).stop();
            } catch (Exception ignored) {
            }
         }
      }
      sites.clear();

      if (stateBaseDir != null) {
         deleteDirectory(stateBaseDir);
      }
   }

   private void waitForIntraSiteCluster(SiteInfo info) {
      await().atMost(30, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
               for (EmbeddedCacheManager manager : info.managers) {
                  int viewSize = manager.getMembers() != null ? manager.getMembers().size() : 0;
                  if (viewSize != info.numNodes) {
                     throw new AssertionError(
                           "Site " + info.name + ": expected " + info.numNodes +
                                 " members but got " + viewSize);
                  }
               }
            });
   }

   private void waitForXSiteViews(List<String> expectedSites) {
      Set<String> expected = Set.copyOf(expectedSites);
      await().atMost(30, TimeUnit.SECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
               for (SiteInfo info : sites.values()) {
                  // Check site master (node 0)
                  EmbeddedCacheManager manager = info.managers.get(0);
                  GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(manager);
                  Transport transport = gcr.getComponent(Transport.class);
                  Set<String> sitesView = transport.getSitesView();
                  if (sitesView == null || !sitesView.equals(expected)) {
                     throw new AssertionError(
                           "Site " + info.name + ": expected sites " + expected +
                                 " but got " + sitesView);
                  }
               }
            });
   }

   private static void replaceTimeService(EmbeddedCacheManager manager, TimeService timeService) {
      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(manager);
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      bcr.replaceComponent(TimeService.class.getName(), timeService, true);
      gcr.rewire();
   }

   private static void deleteDirectory(Path dir) {
      try (var stream = Files.walk(dir)) {
         stream.sorted(Comparator.reverseOrder())
               .forEach(path -> {
                  try {
                     Files.deleteIfExists(path);
                  } catch (IOException ignored) {
                  }
               });
      } catch (IOException ignored) {
      }
   }

   record SiteInfo(String name, List<EmbeddedCacheManager> managers, int numNodes) {
   }
}
