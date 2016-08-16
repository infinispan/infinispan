package org.infinispan.cli;

import java.util.List;

import org.infinispan.cli.commands.ProcessedCommand;

public interface CommandBuffer {
   void reset();

   boolean addCommand(ProcessedCommand commandLine, int nesting);

   List<ProcessedCommand> getBufferedCommands();
}
