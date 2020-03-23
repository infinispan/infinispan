package org.infinispan.persistence.manager;

import java.security.PrivilegedAction;

/**
 * StartThreadAction.
 *
 * @author William Burns
 * @since 11.0
 */
public class StartThreadAction implements PrivilegedAction<Void> {

   private final Runnable task;
   private final String threadName;

   public StartThreadAction(Runnable task, String threadName) {
      this.task = task;
      this.threadName = threadName;
   }

   @Override
   public Void run() {
      Thread thread = new Thread(task, threadName);
      thread.setDaemon(true);
      thread.start();
      return null;
   }
}
