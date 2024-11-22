package org.infinispan.container.versioning;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public final class RandomVersionGenerator implements VersionGenerator {

   public static final RandomVersionGenerator INSTANCE = new RandomVersionGenerator();

   private RandomVersionGenerator() {}

   @Override
   public NumericVersion generateNew() {
      // Hot Rod streaming cache expects a version higher than zero.
      return new NumericVersion(Math.abs(ThreadLocalRandom.current().nextLong()));
   }

   @Override
   public IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      throw new UnsupportedOperationException();
   }

   @Override
   public IncrementableEntryVersion nonExistingVersion() {
      throw new UnsupportedOperationException();
   }

}
