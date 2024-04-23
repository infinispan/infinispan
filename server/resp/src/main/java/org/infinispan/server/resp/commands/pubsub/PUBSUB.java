package org.infinispan.server.resp.commands.pubsub;

import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.FamilyCommand;

/**
 * Family of `<code>PUBSUB</code>` commands.
 *
 * @since 15.0
 * @link <a href="https://redis.io/docs/latest/commands/pubsub/">Redis documentation</a>
 */
public class PUBSUB extends FamilyCommand {

   private static final RespCommand[] PUBSUB_COMMANDS;

   static {
      PUBSUB_COMMANDS = new RespCommand[] {
            new CHANNELS(),
      };
   }

   public PUBSUB() {
      super(-2, 0, 0, 0);
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return PUBSUB_COMMANDS;
   }
}
