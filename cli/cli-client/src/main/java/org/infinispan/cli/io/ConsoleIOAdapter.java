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
      return console.read(prompt).getBuffer();
   }

   @Override
   public String secureReadln(String prompt) throws IOException {
      return console.read(new Prompt(prompt), (char) 0).getBuffer();
   }

   @Override
   public void println(String s) throws IOException {
      console.pushToStdOut(s);
      console.pushToStdOut("\n");
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

}
