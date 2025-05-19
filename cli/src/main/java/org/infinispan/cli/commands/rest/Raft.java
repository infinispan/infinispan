package org.infinispan.cli.commands.rest;

import java.util.concurrent.CompletionStage;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.commands.CliCommand;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "raft", description = "Manages RAFT membership",
      activator = ConnectionActivator.class,
      groupCommands = {
            Raft.AddMember.class,
            Raft.RemoveMember.class,
            Raft.ListMembers.class,
      })
public class Raft extends CliCommand {

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

   public abstract static class RaftMemberCommand extends RestCliCommand {

      @Argument(description = "The raft ID of the member")
      String raftId;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected final boolean isHelp() {
         return help;
      }
   }

   @CommandDefinition(name = "add", description = "Adds a new member to the RAFT cluster.", activator = ConnectionActivator.class)
   public static class AddMember extends RaftMemberCommand {

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws Exception {
         return client.raft().addMember(raftId);
      }
   }

   @CommandDefinition(name = "remove", description = "Removes a member from the RAFT cluster.", activator = ConnectionActivator.class)
   public static class RemoveMember extends RaftMemberCommand {

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws Exception {
         return client.raft().removeMember(raftId);
      }
   }

   @CommandDefinition(name = "list", description = "List the current RAFT membership.", activator = ConnectionActivator.class)
   public static class ListMembers extends RestCliCommand {
      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      protected final boolean isHelp() {
         return help;
      }

      @Override
      protected CompletionStage<RestResponse> exec(ContextAwareCommandInvocation invocation, RestClient client, Resource resource) throws Exception {
         return client.raft().currentMembers();
      }
   }
}
