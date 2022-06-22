package org.infinispan.server.license;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

/**
 * We tried to use
 * Path path = jarfs.getPath("META-INF/", "maven");
 *             Optional<Path> propertyPath = Files.find(path,
 *                   100,
 *                   (filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith("properties"))
 *                   .findFirst();
 * but some artifacts are productized with ANT
 *
 * The META-INF/MANIFEST.MF that comes with ANT is missing the group, artifact and version
 */
public class LicenseTest {

   private static final Pattern TR_REGEX = Pattern.compile("<tr>(.+?)</tr>", Pattern.DOTALL);
   private static final Pattern TD_VALUE_REGEX = Pattern.compile("<td>(.*?)</td>", Pattern.DOTALL);
   private static final String SERVER_OUTPUT_PATH = System.getProperty("server.output.dir");

   @Test
   public void testLicense() throws IOException {

      Set<String> libs = Files.list(getLibDir())
            .filter(Files::isRegularFile)
            .map(Path::getFileName)
            .map(String::valueOf)
            .map(LicenseTest::removeOsName)
            .collect(Collectors.toSet());

      String html = new String(Files.readAllBytes(getDependencyHtmlFile()));
      Matcher trMatcher = TR_REGEX.matcher(html);
      List<String> htmlDependencies = new ArrayList<>();
      while (trMatcher.find()) {
         String trValue = trMatcher.group(1);
         Matcher tdMatcher = TD_VALUE_REGEX.matcher(trValue);
         if (tdMatcher.find()) {
            String group = tdMatcher.group(1);
            String artifact;
            if (group.contains("@patternfly")) {
               // patternfly includes the artifact in the group with a slash
               String[] splitGroup = group.split("/");
               artifact = splitGroup[1];
               // Ignore the next match as it is empty
               tdMatcher.find();
            } else {
               tdMatcher.find();
               artifact = tdMatcher.group(1);
            }

            tdMatcher.find();
            String version = tdMatcher.group(1);

            tdMatcher.find();
            String remoteLicenses = tdMatcher.group(1);
            Assert.assertTrue("Remote License: " + artifact + ":" + version + ":" + remoteLicenses, hasLicenseLink(remoteLicenses));

            tdMatcher.find();
            String localLicenses = tdMatcher.group(1);
            Assert.assertTrue("Local License: " + artifact + ":" + version + ":" + localLicenses, hasLicenseLink(localLicenses));

            htmlDependencies.add(String.format("%s-%s.jar", artifact, version));
         }
      }

      libs.removeAll(htmlDependencies);

      // helpful output
      for (String lib : libs) {
         System.out.println("Missing: " + lib);
      }

      assertEquals(libs.toString(), 0, libs.size());
   }

   private static String removeOsName(String jarFile) {
      return jarFile
            .replace("epoll-linux-x86_64", "epoll")
            .replace("epoll-linux-aarch_64", "epoll")
            .replace("io_uring-linux-x86_64", "io_uring")
            .replace("io_uring-linux-aarch_64", "io_uring");
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

   private boolean hasLicenseLink(String value) {
      return value != null && value.contains("a href");
   }
}
