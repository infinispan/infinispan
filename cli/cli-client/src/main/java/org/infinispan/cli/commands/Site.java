package org.infinispan.cli.commands;

import java.util.Collections;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Option;
import org.infinispan.cli.activators.ConnectionActivator;
import org.infinispan.cli.completers.CacheCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Site.CMD, description = "Manages backup sites",
      activator = ConnectionActivator.class,
      groupCommands = {
            Site.Status.class,
            Site.BringOnline.class,
            Site.TakeOffline.class,
            Site.PushSiteState.class,
            Site.CancelPushState.class,
            Site.CancelReceiveState.class,
            Site.PushSiteStatus.class,
            Site.ClearPushStateStatus.class,
      }
)
public class Site extends CliCommand {

   public static final String CLEAR_PUSH_STATE_STATUS = "clear-push-state-status";
   public static final String PUSH_SITE_STATUS = "push-site-status";
   public static final String CANCEL_RECEIVE_STATE = "cancel-receive-state";
   public static final String CANCEL_PUSH_STATE = "cancel-push-state";
   public static final String PUSH_SITE_STATE = "push-site-state";
   public static final String TAKE_OFFLINE = "take-offline";
   public static final String BRING_ONLINE = "bring-online";
   public static final String STATUS = "status";
   public static final String CMD = "site";
   public static final String SITE_NAME = "site";
   public static final String OP = "op";
   public static final String CACHE = "cache";

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

   @CommandDefinition(name = STATUS, description = "Shows site status", activator = ConnectionActivator.class)
   public static class Status extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, STATUS).arg(CACHE, cache).optionalArg(SITE_NAME, site);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = BRING_ONLINE, description = "Brings a site online", activator = ConnectionActivator.class)
   public static class BringOnline extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, BRING_ONLINE).arg(CACHE, cache).arg(SITE_NAME, site);
         return invocation.execute(cmd);
      }
   }

   @CommandDefinition(name = TAKE_OFFLINE, description = "Takes a site offline", activator = ConnectionActivator.class)
   public static class TakeOffline extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, TAKE_OFFLINE).arg(CACHE, cache).arg(SITE_NAME, site);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = PUSH_SITE_STATE, description = "Starts pushing state to a site", activator = ConnectionActivator.class)
   public static class PushSiteState extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, PUSH_SITE_STATE).arg(CACHE, cache).arg(SITE_NAME, site);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = CANCEL_PUSH_STATE, description = "Cacncels pushing state to a site", activator = ConnectionActivator.class)
   public static class CancelPushState extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, CANCEL_PUSH_STATE).arg(CACHE, cache).arg(SITE_NAME, site);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = CANCEL_RECEIVE_STATE, description = "Cancels receiving state to a site", activator = ConnectionActivator.class)
   public static class CancelReceiveState extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(required = true)
      String site;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, CANCEL_RECEIVE_STATE).arg(CACHE, cache).arg(SITE_NAME, site);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = PUSH_SITE_STATUS, description = "Shows the status of pushing to a site", activator = ConnectionActivator.class)
   public static class PushSiteStatus extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, PUSH_SITE_STATUS).arg(CACHE, cache);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }

   @CommandDefinition(name = CLEAR_PUSH_STATE_STATUS, description = "Clears the push state status", activator = ConnectionActivator.class)
   public static class ClearPushStateStatus extends CliCommand {
      @Option(required = true, completer = CacheCompleter.class)
      String cache;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         CommandInputLine cmd = new CommandInputLine(CMD).arg(OP, CLEAR_PUSH_STATE_STATUS).arg(CACHE, cache);
         return invocation.execute(Collections.singletonList(cmd));
      }
   }
}
