package org.infinispan.commons.graalvm;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class Bundle implements JsonSerialization {

   public static Collection<Bundle> of(String... bundleName) {
      return Arrays.stream(bundleName)
            .map(Bundle::new)
            .collect(Collectors.toList());
   }

   private final String name;

   public Bundle(String name) {
      this.name = name;
   }

   @Override
   public Json toJson() {
      return Json.object().set("name", name);
   }
}
