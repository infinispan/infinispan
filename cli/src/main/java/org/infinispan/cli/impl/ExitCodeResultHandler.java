package org.infinispan.cli.impl;

import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.result.ResultHandler;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class ExitCodeResultHandler implements ResultHandler {
   static int exitCode;

   @Override
   public void onSuccess() {
      exitCode = 0;
   }

   @Override
   public void onFailure(CommandResult result) {
      exitCode = 1;
   }

   @Override
   public void onValidationFailure(CommandResult result, Exception exception) {
      exitCode = 1;
   }

   @Override
   public void onExecutionFailure(CommandResult result, CommandException exception) {
      exitCode = 1;
   }
}
