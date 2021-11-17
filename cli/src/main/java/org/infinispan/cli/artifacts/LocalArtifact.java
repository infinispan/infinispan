package org.infinispan.cli.artifacts;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Represents a local artifact.
 *
 * @since 14.0
 **/
public class LocalArtifact extends AbstractArtifact {
   final Path path;

   public LocalArtifact(String path) {
      this.path = Paths.get(path);
   }

   @Override
   public Path resolveArtifact() {
      if (Files.exists(path)) {
         return path;
      } else {
         return null;
      }
   }
}
