package org.infinispan.cli.commands.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.completers.ConnectorCompleter;
import org.infinispan.cli.completers.IpFilterRuleCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.IpFilterRule;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
@GroupCommandDefinition(name = Connector.CMD, description = "Performs operations on protocol connectors", activator = ConnectionActivator.class, groupCommands = {Connector.Ls.class, Connector.Describe.class, Connector.Start.class, Connector.Stop.class, Connector.IpFilter.class})
public class Connector extends CliCommand {

   public static final String CMD = "connector";
   public static final String TYPE = "type";
   public static final String NAME = "name";

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "ls", description = "Lists connectors", activator = ConnectionActivator.class)
   public static class Ls extends RestCliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().connectorNames();
      }
   }

   @CommandDefinition(name = "describe", description = "Describes a connector", activator = ConnectionActivator.class)
   public static class Describe extends RestCliCommand {

      @Argument(required = true, completer = ConnectorCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().connector(name);
      }
   }

   @CommandDefinition(name = "start", description = "Starts a connector", activator = ConnectionActivator.class)
   public static class Start extends RestCliCommand {

      @Argument(required = true, completer = ConnectorCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().connectorStart(name);
      }
   }

   @CommandDefinition(name = "stop", description = "Stops a connector", activator = ConnectionActivator.class)
   public static class Stop extends RestCliCommand {

      @Argument(required = true, completer = ConnectorCompleter.class)
      String name;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.server().connectorStop(name);
      }
   }

   @GroupCommandDefinition(name = "ipfilter", description = "Manages connector IP filters", activator = ConnectionActivator.class, groupCommands = {IpFilter.Ls.class, IpFilter.Clear.class, IpFilter.Set.class})
   public static class IpFilter extends CliCommand {

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected boolean isHelp() {
         return help;
      }

      @Override
      protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         return CommandResult.FAILURE;
      }

      @CommandDefinition(name = "ls", description = "List all IP filters on a connector", activator = ConnectionActivator.class)
      public static class Ls extends RestCliCommand {

         @Argument(required = true, completer = ConnectorCompleter.class)
         String name;

         @Option(shortName = 'h', hasValue = false, overrideRequired = true)
         protected boolean help;

         @Override
         public boolean isHelp() {
            return help;
         }

         @Override
         protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
            return client.server().connectorIpFilters(name);
         }
      }

      @CommandDefinition(name = "clear", description = "Removes all IP Filters from a connector", activator = ConnectionActivator.class)
      public static class Clear extends RestCliCommand {

         @Argument(required = true, completer = ConnectorCompleter.class)
         String name;

         @Option(shortName = 'h', hasValue = false, overrideRequired = true)
         protected boolean help;

         @Override
         public boolean isHelp() {
            return help;
         }

         @Override
         protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
            return client.server().connectorIpFiltersClear(name);
         }
      }

      @CommandDefinition(name = "set", description = "Sets IP Filters on a connector", activator = ConnectionActivator.class)
      public static class Set extends RestCliCommand {

         @Argument(required = true, completer = ConnectorCompleter.class)
         String name;

         @OptionList(description = "One or more filter rules as \"[ACCEPT|REJECT]/CIDR\"", completer = IpFilterRuleCompleter.class, required = true)
         List<String> rules;

         @Option(shortName = 'h', hasValue = false, overrideRequired = true)
         protected boolean help;

         @Override
         public boolean isHelp() {
            return help;
         }

         @Override
         protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
            List<IpFilterRule> filterRules = new ArrayList<>(rules.size());
            for (String rule : rules) {
               int i = rule.indexOf('/');
               if (i < 0) {
                  throw Messages.MSG.illegalFilterRule(rule);
               }
               IpFilterRule.RuleType ruleType = IpFilterRule.RuleType.valueOf(rule.substring(0, i));
               filterRules.add(new IpFilterRule(ruleType, rule.substring(i + 1)));
            }
            return client.server().connectorIpFilterSet(name, filterRules);
         }
      }
   }
}
