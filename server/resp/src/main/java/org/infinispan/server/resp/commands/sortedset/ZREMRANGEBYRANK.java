package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.ZREMRANGE;

/**
 * ZREMRANGEBYRANK
 *
 * @see <a href="https://redis.io/commands/zremrangebyrank/">ZREMRANGEBYRANK</a>
 * @since 15.0
 */
public class ZREMRANGEBYRANK extends ZREMRANGE {
   public ZREMRANGEBYRANK() {
      super(4, Type.RANK);
   }
}
