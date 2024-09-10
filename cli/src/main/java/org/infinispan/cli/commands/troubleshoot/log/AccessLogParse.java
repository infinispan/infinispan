package org.infinispan.cli.commands.troubleshoot.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Stream;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Arguments;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.converters.LocalDateTimeConverter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;

/**
 * Parses multiple access log files and generate statistics.
 *
 * @author José Bolina
 * @since 15.0
 */
@CommandDefinition(name = "log", description = "Parses access log file and summarizes statistics")
public class AccessLogParse extends CliCommand {

   @Option(shortName = 'o', description = "Operation to filter")
   String operation;

   @OptionList(shortName = 'x', description = "Operations to exclude")
   List<String> excludeOperations;

   @Option(shortName = 't', description = "List the N longest operations")
   int highest;

   @Option(shortName = 'd', description = "List operations with duration greater than or equal to")
   long duration;

   @Option(name = "by-client", description = "Group operations by client", hasValue = false)
   boolean byClient;

   @Option(name = "start", description = "Filter requests only after the given date", converter = LocalDateTimeConverter.class)
   LocalDateTime start;

   @Option(name = "end", description = "Filter requests only before the given date", converter = LocalDateTimeConverter.class)
   LocalDateTime end;

   @Arguments(completer = FileOptionCompleter.class, description = "Path to local access log files")
   List<File> files;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   protected boolean isHelp() {
      return help || files == null;
   }

   @Override
   protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      if (files == null) return CommandResult.FAILURE;

      Stream<AccessLogEntry> stream = readAccessLogs()
            .filter(AccessLogEntry.filterByOperation(operation))
            .filter(AccessLogEntry.ignoreOperations(excludeOperations))
            .filter(AccessLogEntry.filterByDuration(duration))
            .filter(AccessLogEntry.filterExecutionAfter(start))
            .filter(AccessLogEntry.filterExecutionBefore(end));

      AccessLogResult results = highest > 0
            ? getLongestOperations(stream)
            : getStatistics(stream);

      invocation.print(results.prettyPrint(), true);
      return CommandResult.SUCCESS;
   }

   private AccessLogResult getLongestOperations(Stream<AccessLogEntry> stream) {
      return byClient
            ? AccessLogSupport.getLongestOperationsByClient(stream, highest)
            : AccessLogSupport.getLongestGlobalOperations(stream, highest);
   }

   private AccessLogResult getStatistics(Stream<AccessLogEntry> stream) {
      return byClient
            ? AccessLogSupport.getStatisticsByClient(stream)
            : AccessLogSupport.getGlobalStatistics(stream);
   }

   private Stream<AccessLogEntry> readAccessLogs() {
      return files.stream().flatMap(this::readSingleFile);
   }

   private Stream<AccessLogEntry> readSingleFile(File file) {
      String fileName = file.getAbsolutePath();
      return AccessLogParse.uncheckedReadLines(file.toPath())
            .map(l -> AccessLogEntry.newInstance(fileName, l));
   }

   private static Stream<String> uncheckedReadLines(Path path) {
      try {
         return Files.lines(path);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
