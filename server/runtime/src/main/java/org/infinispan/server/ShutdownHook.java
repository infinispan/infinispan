package org.infinispan.server;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public class ShutdownHook extends Thread {
   ExitHandler exitHandler;

   public ShutdownHook(ExitHandler exitHandler) {
      this.exitHandler = exitHandler;
   }

   @Override
   public void run() {
      exitHandler.exit(0);
   }
}
