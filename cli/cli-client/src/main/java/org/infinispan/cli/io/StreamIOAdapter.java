package org.infinispan.cli.io;

import org.infinispan.cli.commands.ProcessedCommand;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class StreamIOAdapter implements IOAdapter {

   @Override
   public boolean isInteractive() {
      return false;
   }

   @Override
   public void println(String s) throws IOException {
      System.out.println(s);
   }

   @Override
   public void error(String s) throws IOException {
      System.err.println(s);
   }

   @Override
   public void result(List<ProcessedCommand> commands, String result, boolean isError) throws IOException {
      if (isError)
         error(result);
      else
         println(result);
   }

   @Override
   public String readln(String s) throws IOException {
      System.out.print(s);
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      return br.readLine();
   }

   @Override
   public String secureReadln(String s) throws IOException {
      //FIXME implement me
      return readln(s);
   }

   @Override
   public int getWidth() {
      return 72;
   }

   @Override
   public void close() {
      //FIXME implement me
   }
}
