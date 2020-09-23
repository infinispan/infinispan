package org.infinispan.cli.commands;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.completers.QueryModeCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = Query.CMD, description = "Queries a cache", activator = ConnectionActivator.class)
public class Query extends CliCommand {
   public static final String CMD = "query";
   public static final String MAX_RESULTS = "max-results";
   public static final String OFFSET = "offset";
   public static final String QUERY_MODE = "query-mode";
   public static final String QUERY = "query";

   @Argument(required = true, description = "The Ickle query")
   String query;

   @Option(completer = CacheCompleter.class)
   String cache;

   @Option(name = MAX_RESULTS, defaultValue = "10")
   Integer maxResults;

   @Option(name = OFFSET, defaultValue = "0")
   Integer offset;

   @Option(name = QUERY_MODE, completer = QueryModeCompleter.class, defaultValue = "FETCH", description = "Mode for queries FETCH|BROADCAST, defaults to FETCH")
   String queryMode;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      CommandInputLine cmd = new CommandInputLine(CMD)
            .arg(QUERY, query)
            .optionalArg(CACHE, cache)
            .option(MAX_RESULTS, maxResults)
            .option(OFFSET, offset)
            .option(QUERY_MODE, queryMode);
      return invocation.execute(cmd);
   }
}
