package org.infinispan.server.test.core.compatibility;

import java.util.ArrayList;
import java.util.List;

public class VersionRange {
   private final List<Restriction> restrictions;

   public VersionRange(String spec) {
      this.restrictions = parseSpec(spec);
   }

   public boolean containsVersion(String version) {
      if (restrictions.isEmpty()) return true; // Accept any if empty
      return restrictions.stream().anyMatch(r -> r.isSatisfiedBy(version));
   }

   private List<Restriction> parseSpec(String spec) {
      List<Restriction> result = new ArrayList<>();
      // Simple splitting for multiple ranges (e.g., [1.0,2.0],[3.0,4.0])
      String[] parts = spec.split("(?<=[])])\\s*,\\s*(?=[(\\[])");

      for (String part : parts) {
         result.add(new Restriction(part.trim()));
      }
      return result;
   }

   private static class Restriction {
      String lowerBound, upperBound;
      boolean lowerInclusive, upperInclusive;

      Restriction(String part) {
         if (part.startsWith("[") || part.startsWith("(")) {
            lowerInclusive = part.startsWith("[");
            upperInclusive = part.endsWith("]");

            String inner = part.substring(1, part.length() - 1);
            String[] bounds = inner.split(",", -1);

            if (bounds.length == 2) {
               lowerBound = bounds[0].trim().isEmpty() ? null : bounds[0].trim();
               upperBound = bounds[1].trim().isEmpty() ? null : bounds[1].trim();
            } else {
               // Exact version [1.2.3]
               lowerBound = upperBound = bounds[0].trim();
            }
         } else {
            // Soft requirement "1.0" - treated as lower bound inclusive, no upper bound
            lowerBound = part;
            lowerInclusive = true;
            upperBound = null;
            upperInclusive = false;
         }
      }

      boolean isSatisfiedBy(String version) {
         int lowRes = (lowerBound == null) ? 1 : compareVersions(version, lowerBound);
         int highRes = (upperBound == null) ? -1 : compareVersions(version, upperBound);

         boolean lowMatch = lowerInclusive ? lowRes >= 0 : lowRes > 0;
         boolean highMatch = upperInclusive ? highRes <= 0 : highRes < 0;

         return lowMatch && highMatch;
      }

      // Simplified Maven version comparison (Major.Minor.Patch)
      private int compareVersions(String v1, String v2) {
         String[] vals1 = v1.split("\\.");
         String[] vals2 = v2.split("\\.");
         int i = 0;
         while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
            i++;
         }
         if (i < vals1.length && i < vals2.length) {
            return Integer.compare(Integer.parseInt(vals1[i]), Integer.parseInt(vals2[i]));
         }
         return Integer.compare(vals1.length, vals2.length);
      }
   }
}
