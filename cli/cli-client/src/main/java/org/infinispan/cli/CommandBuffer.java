package org.infinispan.cli;

import org.infinispan.cli.commands.ProcessedCommand;

import java.util.List;

public interface CommandBuffer {
   void reset();

   boolean addCommand(ProcessedCommand commandLine, int nesting);

   List<ProcessedCommand> getBufferedCommands();
}
