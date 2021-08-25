package org.infinispan.cli.patching;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ServerFile {
   private final Path directory;
   private final String filename;
   private final String basename;
   private final String digest;
   private final String permissions;
   private final boolean soft;

   public ServerFile(Path path, String digest, String permissions, boolean soft) {
      this.directory = path.getParent();
      this.filename = path.getFileName().toString();
      this.basename = basename(filename);
      this.digest = digest;
      this.permissions = permissions;
      this.soft = soft;
   }

   @Override
   public String toString() {
      return "ServerFile{" +
            "directory=" + directory +
            ", filename='" + filename + '\'' +
            ", basename='" + basename + '\'' +
            ", permissions='" + permissions + '\'' +
            ", soft=" + soft +
            '}';
   }

   private static String basename(String filename) {
      // Get the artifact name up to the version
      int l = filename.length();
      for (int i = 0; i < l; i++) {
         char c = filename.charAt(i);
         if (c == '-' && i < l - 1) {
            c = filename.charAt(i + 1);
            if (c >= '0' && c <= '9') {
               return filename.substring(0, i);
            }
         }
      }
      // Could not obtain an artifact
      return filename;
   }

   public String getFilename() {
      return filename;
   }

   public String getDigest() {
      return digest;
   }

   public String getPermissions() {
      return permissions;
   }

   Path getVersionedPath() {
      return directory != null ? directory.resolve(filename) : Paths.get(filename);
   }

   Path getUnversionedPath() {
      return directory != null ? directory.resolve(basename) : Paths.get(basename);
   }

   public boolean isSoft() {
      return soft;
   }
}
