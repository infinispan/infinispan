package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.DIFF;

/**
 * This command is similar to {@link ZDIFFSTORE}, but instead of storing
 * the resulting sorted set, it is returned to the client.
 *
 * Array reply: the result of the difference
 * (optionally with their scores, in case the WITHSCORES option is given).
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zdiff/">Redis Documentation</a>
 */
public class ZDIFF extends DIFF {
   public ZDIFF() {
      super(-3, 0, 0, 0);
   }
}
