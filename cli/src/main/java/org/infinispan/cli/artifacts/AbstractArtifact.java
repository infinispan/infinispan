package org.infinispan.cli.artifacts;

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
}
