package org.infinispan.cli.artifacts;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.cli.util.Utils;

/**
 * Maven artifact coordinate specification.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class MavenArtifact extends AbstractArtifact {
   static final Pattern snapshotPattern = Pattern.compile("-\\d{8}\\.\\d{6}-\\d+$");
   private static final Pattern VALID_PATTERN = Pattern.compile("^([-_a-zA-Z0-9.]+):([-_a-zA-Z0-9.]+):([-_a-zA-Z0-9.]+)(?::([-_a-zA-Z0-9.]+))?$");

   private final String groupId;
   private final String artifactId;
   private final String version;
   private final String classifier;
   private String toString;

   /**
    * Construct a new instance.
    *
    * @param groupId    the group ID (must not be {@code null})
    * @param artifactId the artifact ID (must not be {@code null})
    * @param version    the version string (must not be {@code null})
    * @param classifier the classifier string (must not be {@code null}, may be empty)
    */
   public MavenArtifact(final String groupId, final String artifactId, final String version, final String classifier) {
      this.groupId = groupId;
      this.artifactId = artifactId;
      this.version = version;
      this.classifier = classifier;
   }

   /**
    * Construct a new instance with an empty classifier.
    *
    * @param groupId    the group ID (must not be {@code null})
    * @param artifactId the artifact ID (must not be {@code null})
    * @param version    the version string (must not be {@code null})
    */
   public MavenArtifact(final String groupId, final String artifactId, final String version) {
      this(groupId, artifactId, version, "");
   }

   /**
    * Parse a string and produce artifact coordinates from it.
    *
    * @param string the string to parse (must not be {@code null})
    * @return the artifact coordinates object (not {@code null})
    */
   public static MavenArtifact fromString(String string) {
      final Matcher matcher = VALID_PATTERN.matcher(string);
      if (matcher.matches()) {
         if (matcher.group(4) != null) {
            return new MavenArtifact(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4));
         } else {
            return new MavenArtifact(matcher.group(1), matcher.group(2), matcher.group(3));
         }
      } else {
         throw new IllegalArgumentException(string);
      }
   }

   public static boolean isMavenArtifact(String string) {
      return VALID_PATTERN.matcher(string).matches();
   }

   @Override
   public Path resolveArtifact() throws IOException {
      return resolveArtifact("jar");
   }

   Path resolveArtifact(String packaging) throws IOException {
      String artifactRelativePath = relativeArtifactPath(File.separatorChar);
      String artifactRelativeHttpPath = relativeArtifactPath('/');
      final MavenSettings settings = MavenSettings.getSettings();
      final Path localRepository = settings.getLocalRepository();
      final String pomPath = artifactRelativePath + ".pom";
      if ("pom".equals(packaging)) {
         // ignore classifier
         Path pom = localRepository.resolve(pomPath);
         if (Files.exists(pom)) {
            return pom;
         }
         List<String> remoteRepos = settings.getRemoteRepositories();
         if (remoteRepos.isEmpty()) {
            return null;
         }
         for (String remoteRepository : remoteRepos) {
            try {
               String remotePomPath = remoteRepository + artifactRelativeHttpPath + ".pom";
               Utils.downloadFile(new URL(remotePomPath), pom, verbose, force);
               if (Files.exists(pom)) { //download successful
                  return pom;
               }
            } catch (IOException e) {
               System.out.printf("Could not download '%s' from '%s' repository (%s)%n", artifactRelativePath, remoteRepository, e.getMessage());
               // try next one
            }
         }
      } else {
         String classifier = this.classifier.isEmpty() ? "" : "-" + this.classifier;
         String artifactPath = artifactRelativePath + classifier + "." + packaging;
         Path fp = localRepository.resolve(artifactPath);
         if (Files.exists(fp) && !force) {
            return fp;
         }
         List<String> remoteRepos = settings.getRemoteRepositories();
         if (remoteRepos.isEmpty()) {
            return null;
         }
         final Path artifact = localRepository.resolve(artifactPath);
         final Path pom = localRepository.resolve(pomPath);
         for (String remoteRepository : remoteRepos) {
            try {
               String remotePomPath = remoteRepository + artifactRelativeHttpPath + ".pom";
               String remoteArtifactPath = remoteRepository + artifactRelativeHttpPath + classifier + "." + packaging;
               Utils.downloadFile(new URL(remotePomPath), pom, verbose, force);
               if (!Files.exists(pom)) {
                  // no POM; skip it
                  continue;
               }
               Utils.downloadFile(new URL(remoteArtifactPath), artifact, verbose, force);
               if (Files.exists(artifact)) { //download successful
                  return artifact;
               }
            } catch (IOException e) {
               System.out.printf("Could not download '%s' from '%s' repository%n", artifactRelativePath, remoteRepository);
            }
         }
      }
      //could not find it in remote
      if (verbose) {
         System.out.println("Could not find in any remote repository");
      }
      return null;
   }

   /**
    * Create a relative repository path for the given artifact coordinates.
    *
    * @param separator the separator character to use (typically {@code '/'} or {@link File#separatorChar})
    * @return the path string
    */
   public String relativeArtifactPath(char separator) {
      StringBuilder builder = new StringBuilder(groupId.replace('.', separator));
      builder.append(separator).append(artifactId).append(separator);
      String pathVersion;
      final Matcher versionMatcher = snapshotPattern.matcher(version);
      if (versionMatcher.find()) {
         // it's really a snapshot
         pathVersion = version.substring(0, versionMatcher.start()) + "-SNAPSHOT";
      } else {
         pathVersion = version;
      }
      builder.append(pathVersion).append(separator).append(artifactId).append('-').append(version);
      return builder.toString();
   }

   /**
    * Get the string representation.
    *
    * @return the string representation
    */
   public String toString() {
      String toString = this.toString;
      if (toString == null) {
         final StringBuilder b = new StringBuilder(groupId.length() + artifactId.length() + version.length() + classifier.length() + 16);
         b.append(groupId).append(':').append(artifactId).append(':').append(version);
         if (!classifier.isEmpty()) {
            b.append(':').append(classifier);
         }
         this.toString = toString = b.toString();
      }
      return toString;
   }
}
