package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.ZREMRANGE;

/**
 * ZREMRANGEBYSCORE
 *
 * @see <a href="https://redis.io/commands/zremrangebyscore/">ZREMRANGEBYSCORE</a>
 * @since 15.0
 */
public class ZREMRANGEBYSCORE extends ZREMRANGE {
   public ZREMRANGEBYSCORE() {
      super(4, Type.SCORE);
   }
}
