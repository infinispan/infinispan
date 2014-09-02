package org.infinispan.server.cli.util;

/**
 * A simple command buffer to buffer the Infinispan CLI batches and transactions before sending them to the server.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CliCommandBuffer {

   public static final CliCommandBuffer INSTANCE = new CliCommandBuffer();

   private StringBuilder buffer = new StringBuilder();
   private int nesting = 0;

   private CliCommandBuffer() {
   }

   /**
    * Appends the new command.
    *
    * @param commandString the string with the command and arguments.
    * @param nesting       the command nesting.
    * @return {@code true} if the command(s) in buffer are ready to be sent.
    */
   public final boolean append(String commandString, int nesting) {
      this.nesting += nesting;
      buffer.append(commandString);
      return this.nesting == 0;
   }

   /**
    * @return the commands buffered and clears the buffer.
    */
   public final String getCommandAndReset() {
      String command = buffer.toString();
      buffer = new StringBuilder();
      return command;
   }
}
