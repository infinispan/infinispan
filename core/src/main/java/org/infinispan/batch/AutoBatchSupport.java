package org.infinispan.batch;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.Configuration;

import net.jcip.annotations.NotThreadSafe;

/**
 * Enables for automatic batching.
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
@NotThreadSafe
public abstract class AutoBatchSupport {
   protected BatchContainer batchContainer;

   protected static void assertBatchingSupported(Configuration c) {
      if (!c.invocationBatching().enabled())
         throw new CacheConfigurationException("Invocation batching not enabled in current configuration! Please enable it.");
   }

   protected void startAtomic() {
      batchContainer.startBatch(true);
   }

   protected void endAtomic() {
      batchContainer.endBatch(true, true);
   }

   protected void failAtomic() {
      batchContainer.endBatch(true, false);
   }
}
