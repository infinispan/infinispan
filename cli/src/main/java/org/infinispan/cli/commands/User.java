package org.infinispan.cli.commands;

import static org.infinispan.cli.logging.Messages.MSG;

import java.util.List;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionList;
import org.infinispan.cli.commands.rest.Roles;
import org.infinispan.cli.completers.EncryptionAlgorithmCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.user.UserTool;
import org.infinispan.commons.dataconversion.internal.Json;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "user", description = "User operations", groupCommands = {User.Create.class, User.Describe.class, User.Remove.class, User.Password.class, User.Groups.class, User.Ls.class, User.Encrypt.class, Roles.class})
public class User extends CliCommand {

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

   @CommandDefinition(name = "create", description = "Creates a user", aliases = "add")
   public static class Create extends CliCommand {

      @Argument(description = "The username for the user")
      String username;

      @Option(description = "The password for the user", shortName = 'p')
      String password;

      @Option(description = "The realm ", defaultValue = UserTool.DEFAULT_REALM_NAME, shortName = 'r')
      String realm;

      @OptionList(description = "The algorithms used to encrypt the password", shortName = 'a', completer = EncryptionAlgorithmCompleter.class)
      List<String> algorithms;

      @OptionList(description = "The groups the user should belong to", shortName = 'g')
      List<String> groups;

      @Option(description = "Whether the password should be stored in plain text (not recommended)", name = "plain-text", hasValue = false)
      boolean plainText;

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         try {
            while (username == null || username.isEmpty()) {
               username = invocation.getShell().readLine(MSG.userToolUsername());
            }
         } catch (InterruptedException e) {
            return CommandResult.FAILURE;
         }

         if (password == null) { // Get the password interactively
            try {
               password = invocation.getPasswordInteractively(MSG.userToolPassword(), MSG.userToolPasswordConfirm());
            } catch (InterruptedException e) {
               return CommandResult.FAILURE;
            }
         }
         userTool.createUser(username, password, realm, UserTool.Encryption.valueOf(plainText), groups, algorithms);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "describe", description = "Describes a user")
   public static class Describe extends CliCommand {

      @Argument(description = "The username for the user", required = true)
      String username;

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         invocation.getShell().writeln(userTool.describeUser(username));
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "remove", description = "Removes a user", aliases = "rm")
   public static class Remove extends CliCommand {

      @Argument(description = "The username for the user", required = true)
      String username;

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.removeUser(username);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "password", description = "Changes a user's password")
   public static class Password extends CliCommand {

      @Argument(description = "The username for the user", required = true)
      String username;

      @Option(description = "The password for the user", shortName = 'p')
      String password;

      @Option(description = "The realm ", defaultValue = UserTool.DEFAULT_REALM_NAME, shortName = 'r')
      String realm;

      @OptionList(description = "The algorithms used to encrypt the password", shortName = 'a', completer = EncryptionAlgorithmCompleter.class)
      List<String> algorithms;

      @Option(description = "Whether the password should be stored in plain text", name = "plain-text", hasValue = false)
      boolean plainText;

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (password == null) { // Get the password interactively
            try {
               password = invocation.getPasswordInteractively(MSG.userToolPassword(), MSG.userToolPasswordConfirm());
            } catch (InterruptedException e) {
               return CommandResult.FAILURE;
            }
         }
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.modifyUser(username, password, realm, UserTool.Encryption.valueOf(plainText), null, algorithms);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "groups", description = "Sets a user's groups")
   public static class Groups extends CliCommand {

      @Argument(description = "The username for the user", required = true)
      String username;

      @OptionList(description = "The groups the user should belong to", shortName = 'g', required = true)
      List<String> groups;

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.modifyUser(username, null, null, UserTool.Encryption.DEFAULT, groups, null);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "ls", description = "Lists all users/groups")
   public static class Ls extends CliCommand {

      @Option(description = "Whether to list all unique groups instead of users", shortName = 'g', hasValue = false)
      boolean groups;

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);

         List<String> items;
         if (groups) {
            items = userTool.listGroups();
         } else {
            items = userTool.listUsers();
         }
         invocation.getShell().writeln(Json.make(items).toString());
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "encrypt-all", description = "Encrypts all of the passwords in a property file.")
   public static class Encrypt extends CliCommand {

      @Option(description = "The path of the users.properties file", name = "users-file", shortName = 'f')
      String usersFile;

      @Option(description = "The path of the groups.properties file", name = "groups-file", shortName = 'w')
      String groupsFile;

      @Option(description = "The server root", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @OptionList(description = "The algorithms used to encrypt the password", shortName = 'a', completer = EncryptionAlgorithmCompleter.class)
      List<String> algorithms;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.encryptAll(algorithms);
         return CommandResult.SUCCESS;
      }
   }
}
