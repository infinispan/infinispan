package org.infinispan.server.test.core.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;

public record Compatibility(List<CompatibilityEntry> compatibility) {

   public static Compatibility INSTANCE = Compatibility.load("compatibility.json");

   private static Compatibility load(String name) {
      InputStream is = Compatibility.class.getClassLoader().getResourceAsStream(name);
      if (is != null) {
         try (is) {
            return new ObjectMapper().readValue(is, Compatibility.class);
         } catch (IOException ignored) {
         }
      }
      return new Compatibility(Collections.emptyList());
   }

   public boolean isCompatibilitySkip(RollingUpgradeConfiguration configuration) {
      String callerClass = CallerId.getCallerClass(2).getName();
      String callerMethodName = CallerId.getCallerMethodName(2);
      return isCompatibilitySkip(configuration, callerClass, callerMethodName);
   }

   public boolean isCompatibilitySkip(RollingUpgradeConfiguration configuration, String className, String methodName) {
      Optional<String> found = compatibility.stream()
            .filter(e -> e.matchesVersions(configuration.fromVersion().version(), configuration.toVersion().version()))
            .flatMap(e -> e.exceptions().stream())
            .flatMap(e -> e.testFailures().stream())
            .filter(e -> e.testClass().equals(className))
            .flatMap(e -> e.testMethods().stream())
            .filter(e -> e.equals(methodName))
            .findFirst();
      return found.isPresent();
   }

   public CompatibilityEntry compatibilityEntry(RollingUpgradeConfiguration configuration) {
      return compatibility.stream()
            .filter(e -> e.matchesVersions(configuration.fromVersion().version(), configuration.toVersion().version()))
            .findFirst()
            .orElse(CompatibilityEntry.EMPTY);
   }

   public static void main(String[] args) throws Exception {
      StringBuilder sb = new StringBuilder();
      sb.append("[cols=\"1,1,5\"]\n");
      sb.append("|===\n");
      sb.append("|From version |To version |Exceptions\n\n");

      if (INSTANCE.compatibility() != null) {
         for (CompatibilityEntry entry : INSTANCE.compatibility()) {
            sb.append("|`").append(entry.versionFrom() != null ? entry.versionFrom() : "N/A").append("`\n");
            sb.append("|`").append(entry.versionTo() != null ? entry.versionTo() : "N/A").append("`\n");
            sb.append("a|");

            if (entry.exceptions() != null && !entry.exceptions().isEmpty()) {
               sb.append("!===\n");
               sb.append("!Issue !Description\n\n");

               for (ExceptionDetail ex : entry.exceptions()) {
                  sb.append("! link:")
                        .append(ex.issue())
                        .append("[#")
                        .append(ex.issue().substring(ex.issue().lastIndexOf('/') + 1))
                        .append("]\n");
                  sb.append("! ")
                        .append(escapeAsciiDoc(ex.description()))
                        .append("\n");
               }

               sb.append("!===\n");
            } else {
               sb.append("None\n");
            }
            sb.append("\n");
         }
      }
      sb.append("|===");
      Files.writeString(Paths.get(args[0]), sb);
   }

   private static String escapeAsciiDoc(String text) {
      if (text == null) return "";
      return text.replace("|", "\\|");
   }
}
