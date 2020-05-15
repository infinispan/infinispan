package org.infinispan.server.license;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

public class LicenseTest {

   private static final Pattern TR_REGEX = Pattern.compile("<tr>(.+?)</tr>", Pattern.DOTALL);
   private static final Pattern TD_VALUE_REGEX = Pattern.compile("<td>(.+?)</td>", Pattern.DOTALL);
   private static final String SERVER_OUTPUT_PATH = System.getProperty("server.output.dir");

   @Test
   public void testLicense() throws IOException {

      List<String> libs = new ArrayList<>();
      Files.list(getLibDir())
            .filter(Files::isRegularFile).collect(Collectors.toList())
            .forEach(jar -> libs.add(jar.getFileName().toString()));

      String html = new String(Files.readAllBytes(getDependencyHtmlFile()));
      Matcher trMatcher = TR_REGEX.matcher(html);
      List<String> htmlDependencies = new ArrayList<>();
      while (trMatcher.find()) {
         String trValue = trMatcher.group(1);
         Matcher tdMatcher = TD_VALUE_REGEX.matcher(trValue);
         if (tdMatcher.find()) {
            //String group = tdMatcher.group(1);

            tdMatcher.find();
            String artifact = tdMatcher.group(1);

            tdMatcher.find();
            String version = tdMatcher.group(1);

            htmlDependencies.add(String.format("%s-%s.jar", artifact, version));
         }
      }

      libs.removeAll(htmlDependencies);

      // helpful output
      for (String lib : libs) {
         System.out.println("Missing: " + lib);
      }

      Assert.assertEquals(0, libs.size());
   }

   private Path getLibDir() {
      File libDir = new File(SERVER_OUTPUT_PATH, "lib");
      return libDir.toPath();
   }

   private Path getDependencyHtmlFile() {
      File docsDir = new File(SERVER_OUTPUT_PATH, "docs");
      File licensesDir = new File(docsDir, "licenses");
      File licensesHtmlFile = new File(licensesDir, "licenses.html");
      return licensesHtmlFile.toPath();
   }
}
