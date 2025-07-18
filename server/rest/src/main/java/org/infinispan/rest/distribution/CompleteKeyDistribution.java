package org.infinispan.rest.distribution;

import java.util.List;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record CompleteKeyDistribution(List<KeyDistributionInfo> distribution, boolean containsKeys)  implements JsonSerialization {
   @Override
   public Json toJson() {
      return Json.object()
            .set("contains_key", containsKeys)
            .set("owners", Json.array(distribution.stream().map(KeyDistributionInfo::toJson).toArray()));
   }
}
