package org.infinispan.commons.maven;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * @since 14.0
 **/
public abstract class AbstractArtifact implements Artifact {
   protected boolean verbose;
   protected boolean force;

   @Override
   public Artifact verbose(boolean verbose) {
      this.verbose = verbose;
      return this;
   }

   @Override
   public Artifact force(boolean force) {
      this.force = force;
      return this;
   }

   public static Path downloadFile(URL url, Path dest, boolean verbose, boolean force) throws IOException {
      if (Files.exists(dest)) {
         if (force) {
            Files.delete(dest);
            if (verbose) {
               System.out.printf("Deleting previously downloaded '%s' for '%s'%n", dest, url);
            }
         } else {
            if (verbose) {
               System.out.printf("Using previously downloaded '%s' for '%s'%n", dest, url);
            }
            return dest;
         }
      }
      HttpURLConnection connection = (HttpURLConnection) MavenSettings.init().openConnection(url);

      int statusCode = connection.getResponseCode();
      if (statusCode == HttpURLConnection.HTTP_NOT_FOUND) {
         if (verbose) {
            System.out.printf("'%s' not found%n", url);
         }
         return null;
      }
      if (statusCode == HttpURLConnection.HTTP_MOVED_TEMP
            || statusCode == HttpURLConnection.HTTP_MOVED_PERM) {
         url = new URL(connection.getHeaderField("Location"));
         dest = dest.resolveSibling(getFilenameFromURL(url));
         connection = (HttpURLConnection) url.openConnection();
      }

      try (InputStream bis = connection.getInputStream()) {
         Files.createDirectories(dest.getParent());
         Files.copy(bis, dest, StandardCopyOption.REPLACE_EXISTING);
         if (verbose) {
            System.out.printf("Downloaded '%s' to '%s'%n", url, dest);
         }
         return dest;
      }
   }

   public static String getFilenameFromURL(URL url) {
      String urlPath = url.getPath();
      return urlPath.substring(urlPath.lastIndexOf('/') + 1);
   }
}
