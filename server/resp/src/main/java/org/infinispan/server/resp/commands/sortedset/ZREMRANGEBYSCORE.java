package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.ZREMRANGE;

/**
 * Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
 *
 * Integer reply: the number of elements removed.
 * @since 15.0
 * @see <a href="https://redis.io/commands/zremrangebyscore/">Redis Documentation</a>
 */
public class ZREMRANGEBYSCORE extends ZREMRANGE {
   public ZREMRANGEBYSCORE() {
      super(4, Type.SCORE);
   }
}
