package org.infinispan.server.test.core.compatibility;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.infinispan.commons.util.GlobUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CompatibilityEntry(
      @JsonProperty("version-from") String versionFrom,
      @JsonProperty("version-to") String versionTo,
      Map<String, String> properties,
      List<ExceptionDetail> exceptions
) {
   public static final CompatibilityEntry EMPTY = new CompatibilityEntry(null, null, Collections.emptyMap(), Collections.emptyList());

   public boolean matchesVersions(String from, String to) {
      return Pattern.matches(GlobUtils.globToRegex(versionFrom), from) && Pattern.matches(GlobUtils.globToRegex(versionTo), to);
   }
}
