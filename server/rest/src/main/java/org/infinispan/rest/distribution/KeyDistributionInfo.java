package org.infinispan.rest.distribution;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.KEY_DISTRIBUTION_INFO;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(KEY_DISTRIBUTION_INFO)
public class KeyDistributionInfo implements JsonSerialization, NodeDataDistribution {
   private final String name;
   private final boolean primary;
   private final List<String> addresses;

   @ProtoFactory
   public KeyDistributionInfo(String name, boolean primary, List<String> addresses) {
      this.name = name;
      this.primary = primary;
      this.addresses = addresses;
   }

   @ProtoField(1)
   @Override
   public String name() {
      return name;
   }

   @ProtoField(value = 2, defaultValue = "false")
   public boolean primary() {
      return primary;
   }

   @ProtoField(value = 3, collectionImplementation = ArrayList.class)
   @Override
   public List<String> addresses() {
      return addresses;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("node_name", name)
            .set("primary", primary)
            .set("node_addresses", Json.array(addresses.toArray()));
   }

   public static KeyDistributionInfo resolve(AdvancedCache<?, ?> cache, boolean primary) {
      final CacheManagerInfo manager = cache.getCacheManager().getCacheManagerInfo();
      return new KeyDistributionInfo(manager.getNodeName(), primary, manager.getPhysicalAddressesRaw());
   }
}
