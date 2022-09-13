package org.infinispan.rest.distribution;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.CACHE_DISTRIBUTION_INFO;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stats.Stats;

@ProtoTypeId(CACHE_DISTRIBUTION_INFO)
public class CacheDistributionInfo implements JsonSerialization, NodeDataDistribution {
   private final String name;
   private final List<String> addresses;
   private final long memoryEntries;
   private final long totalEntries;
   private final long memoryUsed;

   @ProtoFactory
   public CacheDistributionInfo(String name, List<String> addresses, long memoryEntries, long totalEntries, long memoryUsed) {
      this.name = name;
      this.addresses = addresses;
      this.memoryEntries = memoryEntries;
      this.totalEntries = totalEntries;
      this.memoryUsed = memoryUsed;
   }

   @ProtoField(1)
   @Override
   public String name() {
      return name;
   }

   @ProtoField(value = 2, collectionImplementation = ArrayList.class)
   @Override
   public List<String> addresses() {
      return addresses;
   }

   @ProtoField(value = 3, defaultValue = "0")
   public long memoryEntries() {
      return memoryEntries;
   }

   @ProtoField(value = 4, defaultValue = "0")
   public long totalEntries() {
      return totalEntries;
   }

   @ProtoField(value = 5, defaultValue = "0")
   public long memoryUsed() {
      return memoryUsed;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("node_name", name)
            .set("node_addresses", Json.array(addresses.toArray()))
            .set("memory_entries", memoryEntries)
            .set("total_entries", totalEntries)
            .set("memory_used", memoryUsed);
   }

   public static CacheDistributionInfo resolve(AdvancedCache<?, ?> cache) {
      final Stats stats = cache.getStats();
      final CacheManagerInfo manager = cache.getCacheManager().getCacheManagerInfo();
      long inMemory = stats.getApproximateEntriesInMemory();
      long total = stats.getApproximateEntries();
      return new CacheDistributionInfo(manager.getNodeName(), manager.getPhysicalAddressesRaw(), inMemory, total,
            stats.getDataMemoryUsed());
   }
}
