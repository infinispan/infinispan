package org.infinispan.hotrod.impl.consistenthash;


import org.infinispan.hotrod.configuration.HotRodConfiguration;
import org.infinispan.commons.util.Util;

/**
 * Factory for {@link org.infinispan.hotrod.impl.consistenthash.ConsistentHash} function. It will try to look
 * into the configuration for consistent hash definitions as follows:
 * consistent-hash.[version]=[fully qualified class implementing ConsistentHash]
 * e.g.
 * <code>infinispan.client.hotrod.hash_function_impl.3=org.infinispan.hotrod.impl.consistenthash.SegmentConsistentHash</code>
 * or if using the {@link HotRodConfiguration} API,
 * <code>configuration.consistentHashImpl(3, org.infinispan.hotrod.impl.consistenthash.SegmentConsistentHash.class);</code>
 * <p/>
 *
 * <p>The defaults are:</p>
 * <ol>
 *    <li>N/A (No longer used.)</li>
 *    <li>org.infinispan.hotrod.impl.ConsistentHashV2</li>
 *    <li>org.infinispan.hotrod.impl.SegmentConsistentHash</li>
 * </ol>
 *
 * @since 14.0
 */
public class ConsistentHashFactory {
   private HotRodConfiguration configuration;

   public void init(HotRodConfiguration configuration) {
      this.configuration = configuration;
   }

   public <T extends ConsistentHash> T newConsistentHash(int version) {
      Class<? extends ConsistentHash> hashFunctionClass = configuration.consistentHashImpl(version);
      // TODO: Why create a brand new instance via reflection everytime a new hash topology is received? Caching???
      return (T) Util.getInstance(hashFunctionClass);
   }
}
