package org.infinispan.cli.shell;

import org.jboss.aesh.console.AeshConsoleCallback;
import org.jboss.aesh.console.ConsoleOperation;

/**
 * CLIConsoleCallback
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class CLIConsoleCallback extends AeshConsoleCallback{
   @Override
   public int execute(ConsoleOperation output) throws InterruptedException {
      return 0;
   }

}
