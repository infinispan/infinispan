package org.infinispan.rest.distribution;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class CompleteKeyDistribution implements JsonSerialization {
   private final List<KeyDistributionInfo> distribution;
   private final boolean containsKeys;

   public CompleteKeyDistribution(List<KeyDistributionInfo> distribution, boolean containsKeys) {
      this.distribution = distribution;
      this.containsKeys = containsKeys;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("contains_key", containsKeys)
            .set("owners", Json.array(distribution.stream().map(KeyDistributionInfo::toJson).toArray()));
   }
}
