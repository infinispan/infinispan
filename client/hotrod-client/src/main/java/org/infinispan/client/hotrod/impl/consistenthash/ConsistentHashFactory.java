package org.infinispan.client.hotrod.impl.consistenthash;


import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.commons.util.Util;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash} function. It will try to look
 * into the configuration for consistent hash definitions as follows:
 * consistent-hash.[version]=[fully qualified class implementing ConsistentHash]
 * e.g.
 * <code>infinispan.client.hotrod.hash_function_impl.1=org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1</code>
 * or if using the {@link Configuration} API,
 * <code>configuration.consistentHashImpl(1, org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1.class);</code>
 * <p/>
 * If no CH function is defined for a certain version, then it will be defaulted to "org.infinispan.client.hotrod.impl.ConsistentHashV[version]".
 * E.g. if the server indicates that in use CH is version 1, and it is not defined within the configuration, it will be defaulted to
 * org.infinispan.client.hotrod.impl.ConsistentHashV1.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashFactory {
   private Class<? extends ConsistentHash>[] version2ConsistentHash;

   public void init(Configuration configuration) {
      this.version2ConsistentHash = configuration.consistentHashImpl();
   }

   public <T extends ConsistentHash> T newConsistentHash(int version) {
      Class<? extends ConsistentHash> hashFunctionClass = version2ConsistentHash[version-1];
      // TODO: Why create a brand new instance via reflection everytime a new hash topology is received? Caching???
      return (T) Util.getInstance(hashFunctionClass);
   }
}
