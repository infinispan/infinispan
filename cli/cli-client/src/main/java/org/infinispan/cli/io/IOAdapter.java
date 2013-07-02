package org.infinispan.cli.io;

import java.io.IOException;

public interface IOAdapter {
   boolean isInteractive();

   void println(String s) throws IOException;

   void error(String s) throws IOException;

   String readln(String s) throws IOException;

   String secureReadln(String s) throws IOException;

   int getWidth();

   void close() throws IOException;
}
