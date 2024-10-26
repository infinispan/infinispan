package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * ZUNIONSTORE
 *
 * @see <a href="https://redis.io/commands/zunionstore/">ZUNIONSTORE</a>
 * @since 15.0
 */
public class ZUNIONSTORE extends AGGCommand {
   public ZUNIONSTORE() {
      super(-4, 1, 1, 1, AGGCommandType.UNION);
   }
}
