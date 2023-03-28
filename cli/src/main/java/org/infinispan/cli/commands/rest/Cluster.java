package org.infinispan.cli.commands.rest;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.internal.ProcessedOption;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionGroup;
import org.aesh.command.parser.OptionParser;
import org.aesh.command.parser.OptionParserException;
import org.aesh.parser.ParsedLineIterator;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author José Bolina
 * @since 15.0
 */
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "cluster", description = "Execute cluster related operations.", activator = ConnectionActivator.class, groupCommands = {Cluster.Start.class})
public class Cluster extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   protected boolean isHelp() {
      return help;
   }

   @Override
   protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "start", description = "SSH into the servers and starts one or more server instances.", activator = ConnectionActivator.class)
   public static class Start extends RestCliCommand {

      @Option(shortName = 'u', description = "Specifies the username to use for SSH.")
      String username;

      @OptionGroup(name = "H", shortName = 'H', description = "Specifies the host to SSH into and custom parameters.", parser = HostAndArgumentsParser.class)
      Map<String, String> hostsAndArguments;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client,
                                                   Resource resource) throws Exception {
         return client.cluster().start(username, hostsAndArguments);
      }

      private static final class HostAndArgumentsParser implements OptionParser {

         @Override
         public void parse(ParsedLineIterator iterator, ProcessedOption currOption) throws OptionParserException {
            // Copied from `AeshOptionParser#processProperty`.
            String word = currOption.isLongNameUsed() ? iterator.pollWord().substring(2) : iterator.pollWord().substring(1);
            String name = currOption.isLongNameUsed() ? currOption.name() : currOption.shortName();

            // Originally, this would enforce the `=` sign.
            if (word.length() < (1 + name.length()))
               throw new OptionParserException("Option "+currOption.getDisplayName()+", must be part of a property");

            if (!word.contains("=")) currOption.addProperty(word.substring(name.length()), "");
            else {
               String propertyName = word.substring(name.length(), word.indexOf("="));
               String value = word.substring(word.indexOf("=") + 1);

               // Originally, this would require value to be non-empty.
               currOption.addProperty(propertyName, value);
            }
         }
      }
   }
}
