package org.infinispan.container.versioning;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Generates unique numeric versions for both local and clustered environments.
 * When used on clustered caches, node information is used to guarantee versions
 * are unique cluster-wide.
 *
 * If the cache is configured to be local, the version generated is based
 * around an atomic counter. On the contrary, if the cache is clustered, the
 * generated version is composed of:
 * [view id (2 bytes)][rank (2 bytes)][version counter (4 bytes)], where rank
 * refers to the position of this node within the view.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Scope(Scopes.NAMED_CACHE)
public class NumericVersionGenerator implements VersionGenerator {
   // TODO: Possibly seed version counter on capped System.currentTimeMillis, to avoid issues with clients holding to versions in between restarts
   final AtomicInteger versionCounter = new AtomicInteger();
   private static final NumericVersion NON_EXISTING = new NumericVersion(0);

   @Inject Configuration configuration;
   @Inject RankCalculator rankCalculator;

   private boolean isClustered;

   @Start
   public void start() {
      isClustered = configuration.clustering().cacheMode().isClustered();
   }

   @Override
   public IncrementableEntryVersion generateNew() {
      long counter = versionCounter.incrementAndGet();
      return createNumericVersion(counter);

   }

   private IncrementableEntryVersion createNumericVersion(long counter) {
      // Version counter occupies the least significant 4 bytes of the version
      return isClustered
            ? new NumericVersion(rankCalculator.getVersionPrefix() | counter)
            : new NumericVersion(counter);
   }

   @Override
   public IncrementableEntryVersion increment(IncrementableEntryVersion initialVersion) {
      if (initialVersion instanceof NumericVersion) {
         NumericVersion old = (NumericVersion) initialVersion;
         long counter = old.getVersion() + 1;
         return createNumericVersion(counter);
      }

      throw CONTAINER.unexpectedInitialVersion(initialVersion.getClass().getName());
   }

   @Override
   public IncrementableEntryVersion nonExistingVersion() {
      return NON_EXISTING;
   }

   void resetCounter() {
      versionCounter.set(0);
   }
}
