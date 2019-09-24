package org.infinispan.cli.impl;

import org.aesh.command.CommandNotFoundHandler;
import org.aesh.command.shell.Shell;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CliCommandNotFoundHandler implements CommandNotFoundHandler {
   @Override
   public void handleCommandNotFound(String line, Shell shell) {
      shell.writeln("Command not found");
   }
}
