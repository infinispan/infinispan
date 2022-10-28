package org.infinispan.cli.impl;

import java.io.Console;
import java.io.IOException;
import java.nio.charset.Charset;

import org.aesh.readline.Prompt;
import org.aesh.readline.terminal.Key;
import org.aesh.readline.tty.terminal.TerminalConnection;
import org.aesh.readline.util.Parser;
import org.aesh.terminal.utils.ANSI;

public class KubeShell extends AeshDelegatingShell {

   public KubeShell() throws IOException {
      super(new TerminalConnection(Charset.defaultCharset(), System.in, System.out));
   }

   @Override
   public void write(String out, boolean paging) {
      System.out.print(out);
   }

   @Override
   public void writeln(String out, boolean paging) {
      System.out.println(out);
   }

   @Override
   public void write(int[] out) {
      Console console = System.console();
      if (console != null) {
         console.writer().write(Parser.fromCodePoints(out));
         console.writer().flush();
      }
   }

   @Override
   public void write(char out) {
      System.out.println(out);
   }

   @Override
   public String readLine() {
      return readLine(new Prompt());
   }

   @Override
   public String readLine(Prompt prompt) {
      Console console = System.console();
      if (console != null) {
         if (prompt != null) {
            console.writer().print(Parser.fromCodePoints(prompt.getANSI()));
            console.writer().flush();
            if (prompt.isMasking()) {
               return new String(console.readPassword());
            }
         }
         return console.readLine();
      }
      return null;
   }

   @Override
   public Key read() {
      return read(null);
   }

   @Override
   public Key read(Prompt prompt) {
      Console console = System.console();
      if (console != null) {
         try {
            if (prompt != null) {
               console.writer().print(Parser.fromCodePoints(prompt.getANSI()));
               console.writer().flush();
            }
            int input = console.reader().read();
            return Key.getKey(new int[]{input});
         } catch (IOException e) {
            e.printStackTrace();
         }
      }
      return null;
   }

   @Override
   public void clear() {
      Console console = System.console();
      if (console != null) {
         console.writer().write(Parser.fromCodePoints(ANSI.CLEAR_SCREEN));
      }
   }
}
