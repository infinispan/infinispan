package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.DIFF;

/**
 * ZDIFF
 *
 * @see <a href="https://redis.io/commands/zdiff/">ZDIFF</a>
 * @since 15.0
 */
public class ZDIFF extends DIFF {
   public ZDIFF() {
      super(-3, 0, 0, 0);
   }
}
