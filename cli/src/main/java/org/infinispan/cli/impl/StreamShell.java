package org.infinispan.cli.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import org.aesh.command.shell.Shell;
import org.aesh.readline.Prompt;
import org.aesh.readline.terminal.Key;
import org.aesh.readline.util.Parser;
import org.aesh.terminal.tty.Size;
import org.infinispan.commons.util.Util;

/**
 * @since 14.0
 **/
public class StreamShell implements Shell {
   private final BufferedReader in;
   private final PrintStream out;

   public StreamShell() {
      this(System.in, System.out);
   }

   public StreamShell(InputStream in, PrintStream out) {
      this.in = new BufferedReader(new InputStreamReader(in));
      this.out = out;
   }

   @Override
   public void write(String msg, boolean paging) {
      out.print(msg);
   }

   @Override
   public void writeln(String msg, boolean paging) {
      out.println(msg);
   }

   @Override
   public void write(int[] cp) {
      out.print(Parser.fromCodePoints(cp));
      out.flush();
   }

   @Override
   public void write(char c) {
      out.print(c);
   }

   @Override
   public String readLine() throws InterruptedException {
      try {
         String line = in.readLine();
         if (line == null) {
            Util.close(in);
         }
         return line;
      } catch (IOException e) {
         return null;
      }
   }

   @Override
   public String readLine(Prompt prompt) throws InterruptedException {
      return readLine();
   }

   @Override
   public Key read() throws InterruptedException {
      try {
         int input = in.read();
         return Key.getKey(new int[]{input});
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public Key read(Prompt prompt) throws InterruptedException {
      return read();
   }

   @Override
   public boolean enableAlternateBuffer() {
      return false;
   }

   @Override
   public boolean enableMainBuffer() {
      return false;
   }

   @Override
   public Size size() {
      return new Size(0, 0);
   }

   @Override
   public void clear() {
   }
}
