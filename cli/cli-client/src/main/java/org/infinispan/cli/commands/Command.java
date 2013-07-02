package org.infinispan.cli.commands;

import java.util.List;

import org.infinispan.cli.Context;

public interface Command {
   String getName();

   List<String> getOptions();

   boolean isAvailable(Context context);

   void execute(Context context, ProcessedCommand commandLine);

   void complete(Context context, ProcessedCommand procCmd, List<String> candidates);
}
