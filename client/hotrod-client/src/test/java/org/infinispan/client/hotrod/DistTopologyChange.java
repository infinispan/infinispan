package org.infinispan.client.hotrod;

import org.infinispan.config.Configuration;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class DistTopologyChange extends ReplTopologyChangeTest {
   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.DIST_SYNC;
   }
}
