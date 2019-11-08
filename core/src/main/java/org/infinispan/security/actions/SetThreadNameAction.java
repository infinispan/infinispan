package org.infinispan.security.actions;

import java.security.PrivilegedAction;

/**
 * SetThreadNameAction.
 *
 * @author Ryan Emerson
 * @since 10.1
 */
public class SetThreadNameAction implements PrivilegedAction<Void> {

   private final String threadName;

   public SetThreadNameAction(String threadName) {
      this.threadName = threadName;
   }

   @Override
   public Void run() {
      Thread.currentThread().setName(threadName);
      return null;
   }
}
