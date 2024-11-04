package org.infinispan.server.resp.commands.scripting;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.FamilyCommand;

/**
 * SCRIPT
 *
 * @see <a href="https://redis.io/docs/latest/commands/script/">SCRIPT</a>
 * @since 15.1
 */
public class SCRIPT extends FamilyCommand {
   private static final RespCommand[] SCRIPT_COMMANDS;

   static {
      SCRIPT_COMMANDS = new RespCommand[] {
            new DEBUG(),  //SCRIPT DEBUG <YES | SYNC | NO>
            new EXISTS(),   //SCRIPT EXISTS sha1 [sha1 ...]
            new FLUSH(),  //SCRIPT FLUSH [ASYNC | SYNC]
            new KILL(), //SCRIPT KILL
            new LOAD()     //SCRIPT LOAD script
      };
   }

   public SCRIPT() {
      super(-2, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SLOW;
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return SCRIPT_COMMANDS;
   }

}
