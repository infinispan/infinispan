package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.DIFF;

/**
 * Computes the difference between the first and all successive input sorted sets and stores
 * the result in destination. The total number of input keys is specified by numkeys.
 *
 * Keys that do not exist are considered to be empty sets.
 *
 * Integer reply: the number of elements in the resulting sorted set at destination.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zdiffstore/">Redis Documentation</a>
 */
public class ZDIFFSTORE extends DIFF {
   public ZDIFFSTORE() {
      super(-4, 1, 1, 1);
   }
}
