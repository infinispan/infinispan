package org.infinispan.manager;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;

/**
 * @since 10.0
 */
public class CacheManagerInfo {

   public static final List<String> LOCAL_NODE = Collections.singletonList("local");
   private final DefaultCacheManager cacheManager;
   private final ConfigurationManager configurationManager;
   private final InternalCacheRegistry internalCacheRegistry;

   public CacheManagerInfo(DefaultCacheManager cacheManager,
                           ConfigurationManager configurationManager, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      this.configurationManager = configurationManager;
      this.internalCacheRegistry = internalCacheRegistry;

   }

   public String getCoordinatorAddress() {
      Transport t = cacheManager.getTransport();
      return t == null ? "N/A" : t.getCoordinator().toString();
   }

   public boolean isCoordinator() {
      return cacheManager.getTransport() != null && cacheManager.getTransport().isCoordinator();
   }

   public String getCacheManagerStatus() {
      return cacheManager.getStatus().toString();
   }

   public Set<BasicCacheInfo> getDefinedCaches() {
      return cacheManager.getCacheNames().stream().map(c -> {
         boolean started = cacheManager.getCaches().containsKey(c);
         return new BasicCacheInfo(c, started);
      }).collect(Collectors.toSet());
   }

   public Set<String> getCacheConfigurationNames() {
      Set<String> names = new HashSet<>(configurationManager.getDefinedConfigurations());
      internalCacheRegistry.filterPrivateCaches(names);
      if (names.isEmpty())
         return Collections.emptySet();
      else
         return Immutables.immutableSetWrap(names);
   }

   public long getCreatedCacheCount() {
      return cacheManager.getCaches().keySet().stream().filter(c -> !internalCacheRegistry.isInternalCache(c)).count();
   }

   public long getRunningCacheCount() {
      return cacheManager.getCaches().keySet().stream().filter(c -> cacheManager.isRunning(c) && !internalCacheRegistry.isInternalCache(c)).count();
   }

   public String getVersion() {
      return Version.getVersion();
   }

   public String getName() {
      return configurationManager.getGlobalConfiguration().cacheManagerName();
   }

   public String getNodeAddress() {
      return cacheManager.getLogicalAddressString();
   }

   public String getPhysicalAddresses() {
      if (cacheManager.getTransport() == null) return "local";
      List<Address> address = cacheManager.getTransport().getPhysicalAddresses();
      return address == null ? "local" : address.toString();
   }

   public List<String> getClusterMembers() {
      if (cacheManager.getTransport() == null) return LOCAL_NODE;
      return cacheManager.getTransport().getMembers().stream().map(Objects::toString).collect(Collectors.toList());
   }

   public List<String> getClusterMembersPhysicalAddresses() {
      if (cacheManager.getTransport() == null) return LOCAL_NODE;
      List<Address> addressList = cacheManager.getTransport().getMembersPhysicalAddresses();
      return addressList.stream().map(Objects::toString).collect(Collectors.toList());
   }

   public int getClusterSize() {
      if (cacheManager.getTransport() == null) return 1;
      return cacheManager.getTransport().getMembers().size();
   }

   public String getClusterName() {
      return configurationManager.getGlobalConfiguration().transport().clusterName();
   }

   public String getLocalSite() {
      if (cacheManager.getTransport() == null) return null;
      return cacheManager.getTransport().localSiteName();
   }

   private String getLogicalAddressString() {
      return cacheManager.getAddress() == null ? "local" : cacheManager.getAddress().toString();
   }

   static class BasicCacheInfo {
      String name;
      boolean started;

      BasicCacheInfo(String name, boolean started) {
         this.name = name;
         this.started = started;
      }

      public String getName() {
         return name;
      }

      public boolean isStarted() {
         return started;
      }
   }
}
