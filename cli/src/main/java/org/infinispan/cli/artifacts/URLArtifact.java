package org.infinispan.cli.artifacts;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.cli.util.Utils;
import org.infinispan.commons.util.Version;

/**
 * Represents a generic URL artifact.
 *
 * @since 14.0
 **/
public class URLArtifact extends AbstractArtifact {
   final URL url;

   public URLArtifact(String path) {
      try {
         url = new URL(path);
      } catch (MalformedURLException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public Path resolveArtifact() throws IOException {
      Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), Version.getBrandName().toLowerCase().replace(' ', '_'), "cache");
      Files.createDirectories(tmpDir);
      Path dest = tmpDir.resolve(Utils.getFilenameFromURL(url));
      return Utils.downloadFile(url, dest, verbose, force);
   }
}
