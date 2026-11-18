package org.infinispan.server.resp.commands.stream;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.FamilyCommand;

/**
 * XINFO
 *
 * @see <a href="https://redis.io/commands/xinfo/">XINFO</a>
 * @since 16.4
 */
public class XINFO extends FamilyCommand {

   private static final RespCommand[] XINFO_COMMANDS = new RespCommand[]{
         new STREAM(),
   };

   public XINFO() {
      super(-3, 0, 0, 0, AclCategory.READ.mask() | AclCategory.STREAM.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return XINFO_COMMANDS;
   }
}
