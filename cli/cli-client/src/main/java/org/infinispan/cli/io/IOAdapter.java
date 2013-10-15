package org.infinispan.cli.io;

import org.infinispan.cli.commands.ProcessedCommand;

import java.io.IOException;
import java.util.List;

public interface IOAdapter {
   boolean isInteractive();

   void println(String s) throws IOException;

   void error(String s) throws IOException;

   void result(List<ProcessedCommand> commands, String result, boolean isError) throws IOException;

   String readln(String s) throws IOException;

   String secureReadln(String s) throws IOException;

   int getWidth();

   void close() throws IOException;
}
