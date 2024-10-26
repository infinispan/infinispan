package org.infinispan.server.resp.commands.sortedset;

import org.infinispan.server.resp.commands.sortedset.internal.AGGCommand;

/**
 * ZINTER
 *
 * @see <a href="https://redis.io/commands/sinter/">ZINTER</a>
 * @since 15.0
 */
public class ZINTER extends AGGCommand {
   public ZINTER() {
      super(-3, 0, 0, 0, AGGCommandType.INTER);
   }
}
