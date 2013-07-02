package org.infinispan.cli.commands;

import java.util.Collections;
import java.util.List;

import org.infinispan.cli.Context;

public abstract class AbstractCommand implements Command {

   private static final List<String> NO_OPTIONS = Collections.emptyList();

   @Override
   public List<String> getOptions() {
      return NO_OPTIONS;
   }

   @Override
   public void complete(Context context, ProcessedCommand procCmd, List<String> candidates) {
      // Do nothing
   }

}
