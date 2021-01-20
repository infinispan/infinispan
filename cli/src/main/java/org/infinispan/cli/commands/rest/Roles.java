package org.infinispan.cli.commands.rest;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "roles", description = "Manages security roles", activator = ConnectionActivator.class, groupCommands = {Roles.Ls.class, Roles.Grant.class, Roles.Deny.class})
public class Roles extends CliCommand {

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation commandInvocation) {
      commandInvocation.println(commandInvocation.getHelpInfo());
      return CommandResult.SUCCESS;
   }

   @CommandDefinition(name = "ls", description = "Lists roles assigned to principals")
   public static class Ls extends RestCliCommand {

      @Argument(description = "The principal for which the roles should be listed. If unspecified all available roles are listed.")
      String principal;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.security().listRoles(principal);
      }
   }

   @CommandDefinition(name = "grant", description = "Grants roles to principals")
   public static class Grant extends RestCliCommand {

      @Argument(description = "The principal to which the roles should be granted", required = true)
      String principal;

      @OptionList(shortName = 'r', required = true)
      List<String> roles;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.security().grant(principal, roles);
      }
   }

   @CommandDefinition(name = "deny", description = "Denies roles to principals")
   public static class Deny extends RestCliCommand {

      @Argument(description = "The principal to which the roles should be denied", required = true)
      String principal;

      @OptionList(shortName = 'r', required = true)
      List<String> roles;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) {
         return client.security().deny(principal, roles);
      }
   }
}
