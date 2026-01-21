package org.infinispan.server.test.core.compatibility;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CompatibilityEntry(
      @JsonProperty("version-from") String versionFrom,
      @JsonProperty("version-to") String versionTo,
      Map<String, String> properties,
      List<ExceptionDetail> exceptions
) {
   public static final CompatibilityEntry EMPTY = new CompatibilityEntry(null, null, Collections.emptyMap(), Collections.emptyList());

   public boolean matchesVersions(String from, String to) {
      return new VersionRange(versionFrom).containsVersion(from) && new VersionRange(versionTo).containsVersion(to);
   }
}
