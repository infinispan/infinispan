package org.infinispan.cli.impl;

import org.aesh.command.settings.QuitHandler;
import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ContextAwareQuitHandler implements QuitHandler {
   private final Context context;

   public ContextAwareQuitHandler(Context context) {
      this.context = context;
   }

   @Override
   public void quit() {
      context.disconnect();
   }
}
