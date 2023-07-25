package org.infinispan.server.resp.tx;

import java.util.List;

import org.infinispan.server.resp.RespCommand;
class TransactionCommand {

   private final RespCommand command;
   private final List<byte[]> arguments;

   public TransactionCommand(RespCommand command, List<byte[]> arguments) {
      this.command = command;
      this.arguments = arguments;
   }

   public RespCommand getCommand() {
      return command;
   }

   public List<byte[]> getArguments() {
      return arguments;
   }
}
