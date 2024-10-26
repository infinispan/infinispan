package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.ZREMRANGE;

/**
 * ZREMRANGEBYLEX
 *
 * @see <a href="https://redis.io/commands/zremrangebylex/">ZREMRANGEBYLEX</a>
 * @since 15.0
 */
public class ZREMRANGEBYLEX extends ZREMRANGE {
   public ZREMRANGEBYLEX() {
      super(4, Type.LEX);
   }
}
