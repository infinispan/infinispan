package org.infinispan.server.resp.commands.pubsub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

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
            new NUMPAT(),
      };
   }

   public PUBSUB() {
      super(-2, 0, 0, 0);
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return PUBSUB_COMMANDS;
   }

   static Predicate<byte[]> deduplicate() {
      List<byte[]> channels = new ArrayList<>(4);
      return channel -> {
         for (byte[] bytes : channels) {
            if (Arrays.equals(channel, bytes))
               return false;
         }
         channels.add(channel);
         return true;
      };
   }
}
