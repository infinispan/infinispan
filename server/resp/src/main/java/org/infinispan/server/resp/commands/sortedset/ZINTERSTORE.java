package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * ZINTERSTORE
 *
 * @see <a href="https://redis.io/commands/zinterstore/">ZINTERSTORE</a>
 * @since 15.0
 */
public class ZINTERSTORE extends AGGCommand {
   public ZINTERSTORE() {
      super(-4, 1, 1, 1, AGGCommandType.INTER);
   }
}
