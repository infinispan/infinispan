package org.infinispan.cli.impl;

import java.util.function.Consumer;

import org.aesh.command.CommandNotFoundHandler;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CliCommandNotFoundHandler implements CommandNotFoundHandler {
   @Override
   public void handleCommandNotFound(String line, Consumer<String> output) {
      output.accept("Command not found");
   }
}
