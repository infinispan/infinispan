package org.infinispan.commons.graalvm;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class Resource implements JsonSerialization {

   public static Collection<Resource> of(String... files) {
      return Arrays.stream(files)
            .map(Resource::new)
            .collect(Collectors.toList());
   }

   final String pattern;

   public Resource(String pattern) {
      this.pattern = pattern;
   }

   @Override
   public Json toJson() {
      return Json.object().set("pattern", pattern);
   }
}
