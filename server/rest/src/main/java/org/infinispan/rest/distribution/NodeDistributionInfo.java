package org.infinispan.rest.distribution;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.CLUSTER_DISTRIBUTION_INFO;
import static org.infinispan.stats.impl.LocalContainerStatsImpl.LOCAL_CONTAINER_STATS;

import java.util.ArrayList;
import java.util.List;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stats.ClusterContainerStats;

/**
 * Collect node's information and statistics of the local JVM.
 * <p/>
 * Contains information about the node name and address; the JVM statistics are memory used and available,
 * all in bytes. We rely on {@link ClusterContainerStats} to retrieve the JVM values, so if the collector is
 * disabled, we return -1.
 *
 * @author José Bolina
 * @see 14.0
 */
@Immutable
@ProtoTypeId(CLUSTER_DISTRIBUTION_INFO)
public class NodeDistributionInfo implements JsonSerialization, NodeDataDistribution {
   private final String name;
   private final List<String> addresses;
   private final long memoryAvailable;
   private final long memoryUsed;

   @ProtoFactory
   public NodeDistributionInfo(String name, List<String> addresses, long memoryAvailable, long memoryUsed) {
      this.name = name;
      this.addresses = addresses;
      this.memoryAvailable = memoryAvailable;
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
   public long memoryAvailable() {
      return memoryAvailable;
   }

   @ProtoField(value = 4, defaultValue = "0")
   public long memoryUsed() {
      return memoryUsed;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("node_name", name)
            .set("node_addresses", Json.array(addresses.toArray()))
            .set("memory_available", memoryAvailable)
            .set("memory_used", memoryUsed);
   }

   public static NodeDistributionInfo resolve(CacheManagerInfo manager, GlobalComponentRegistry registry) {
      String name = manager.getNodeName();
      List<String> addresses = manager.getPhysicalAddressesRaw();
      final ClusterContainerStats stats = registry.getComponent(ClusterContainerStats.class, LOCAL_CONTAINER_STATS);
      return new NodeDistributionInfo(name, addresses, stats.getMemoryAvailable(), stats.getMemoryUsed());
   }
}
