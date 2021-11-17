package org.infinispan.cli.artifacts;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @since 14.0
 **/
public interface Artifact {

   static Artifact fromString(String name) {
      if ((name.startsWith("http://")) || name.startsWith("https://") || name.startsWith("file://") || name.startsWith("ftp://")) {
         return new URLArtifact(name);
      } else if (MavenArtifact.isMavenArtifact(name)) {
         return MavenArtifact.fromString(name);
      } else {
         return new LocalArtifact(name);
      }
   }

   Path resolveArtifact() throws IOException;

   Artifact verbose(boolean verbose);

   Artifact force(boolean force);
}
