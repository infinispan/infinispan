package org.infinispan.cli.io;

import java.io.IOException;
import java.util.List;

import org.fusesource.jansi.Ansi;
import org.infinispan.cli.commands.ProcessedCommand;
import org.jboss.aesh.console.Console;
import org.jboss.aesh.console.Prompt;

public class ConsoleIOAdapter implements IOAdapter {
   private final Console console;

   public ConsoleIOAdapter(final Console console) {
      this.console = console;
   }

   @Override
   public boolean isInteractive() {
      return true;
   }

   @Override
   public String readln(String prompt) throws IOException {
      return read(prompt, null);
   }

   @Override
   public String secureReadln(String prompt) throws IOException {
      return read(prompt, (char)0);
   }

   @Override
   public void println(String s) throws IOException {
      console.getShell().out().println(s);
   }

   @Override
   public void error(String s) throws IOException {
      Ansi ansi = new Ansi();
      ansi.fg(Ansi.Color.RED);
      println(ansi.render(s).reset().toString());
   }

   @Override
   public void result(List<ProcessedCommand> commands, String result, boolean isError) throws IOException {
      if (isError)
         error(result);
      else
         println(result);
   }

   @Override
   public int getWidth() {
      return console.getTerminalSize().getWidth();
   }

   @Override
   public void close() throws IOException {
      console.stop();
   }

   private String read(String prompt, Character mask) {
      Prompt origPrompt = null;
      if (!console.getPrompt().getPromptAsString().equals(prompt)) {
         origPrompt = console.getPrompt();
         console.setPrompt(new Prompt(prompt, mask));
      }
      try {
         return console.getInputLine();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } finally {
         if (origPrompt != null) {
            console.setPrompt(origPrompt);
         }
      }
      return null;
   }


}
