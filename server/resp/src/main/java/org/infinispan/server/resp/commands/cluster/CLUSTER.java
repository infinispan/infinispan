package org.infinispan.server.resp.commands.cluster;

import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.commands.FamilyCommand;

public class CLUSTER extends FamilyCommand {

   private static final RespCommand[] CLUSTER_COMMANDS;

   static {
      CLUSTER_COMMANDS = new RespCommand[] {
            new SHARDS()
      };
   }

   public CLUSTER() {
      super(-2, 0, 0, 0);
   }

   @Override
   public RespCommand[] getFamilyCommands() {
      return CLUSTER_COMMANDS;
   }
}
