package org.infinispan.cli.test;

public class BatchCliTerminal extends CliTerminal {

   public BatchCliTerminal(int exitCode, String output) {
      this.exitCode = exitCode;
      synchronized (this.output) {
         this.output.append(output);
      }
   }

   @Override
   public void send(String data) {
      throw new UnsupportedOperationException("This terminal is not interactive");
   }

   @Override
   public void close() {
   }
}
