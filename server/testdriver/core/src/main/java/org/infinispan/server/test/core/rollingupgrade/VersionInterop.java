package org.infinispan.server.test.core.rollingupgrade;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.commons.maven.MavenArtifact;
import org.infinispan.server.test.core.Unzip;
import org.infinispan.testing.Testing;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

final class VersionInterop {

   private static final String GROUP_ID = "org.infinispan";
   private static final String ARTIFACT_ID = "infinispan-server-tests";
   private static final String ARTIFACT_CLASSIFIER = "artifacts";
   private static final String CONFIGURATIONS_CLASSIFIER = "configurations";

   public static Archive<?>[] loadVersionedArtifacts(String version) {
      try {
         MavenArtifact mavenArtifacts = new MavenArtifact(GROUP_ID, ARTIFACT_ID, version, ARTIFACT_CLASSIFIER);
         Path artifactsZip = mavenArtifacts.resolveArtifact("zip");
         if (artifactsZip == null) {
            RollingUpgradeHandler.log.warnf("Could not download custom artifacts for version %s using %s. Failures may happen", version, mavenArtifacts);
            return null;
         }

         RollingUpgradeHandler.log.infof("Custom artifacts for version %s using %s", version, mavenArtifacts);
         return Unzip.unzip(artifactsZip, Paths.get(Testing.tmpDirectory(), version))
               .stream().map(p -> ShrinkWrap.createFromZipFile(JavaArchive.class, p.toFile()))
               .toArray(i -> new Archive<?>[i]);
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   public static Path loadVersionedConfigurations(String version) {
      try {
         MavenArtifact mavenArtifacts =  new MavenArtifact(GROUP_ID, ARTIFACT_ID, version, CONFIGURATIONS_CLASSIFIER);
         Path configurationsZip = mavenArtifacts.resolveArtifact("zip");
         if (configurationsZip == null) {
            RollingUpgradeHandler.log.warnf("Could not download custom configurations for version %s using %s. Failures may happen", version, mavenArtifacts);
            return null;
         }

         RollingUpgradeHandler.log.infof("Configurations for version %s using %s", version, mavenArtifacts);
         Path target = Paths.get(Testing.tmpDirectory(), version);
         Unzip.unzip(configurationsZip, target);
         return target;
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }
}
