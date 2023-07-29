package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * This command is similar to {@link ZUNIONSTORE}, but instead of storing the resulting sorted set,
 * it is returned to the client.
 *
 * For a description of the WEIGHTS and AGGREGATE options, see {@link ZUNIONSTORE}.
 *
 * Array reply: the result of union (optionally with their scores,
 * in case the WITHSCORES option is given).
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/zunion/">Redis Documentation</a>
 */
public class ZUNION extends AGGCommand {
   public ZUNION() {
      super(-3, 0, 0, 0, AGGCommandType.UNION);
   }
}
