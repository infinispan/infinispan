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
import org.aesh.readline.Prompt;
import org.infinispan.cli.completers.EncryptionAlgorithmCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.user.UserTool;
import org.kohsuke.MetaInfServices;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = User.CMD, description = "User operations", groupCommands = {User.Create.class, User.Describe.class, User.Remove.class, User.Password.class, User.Groups.class, User.Ls.class, User.Encrypt.class})
public class User extends CliCommand {

   public static final String CMD = "user";

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      // This command serves only to wrap the sub-commands
      invocation.println(invocation.getHelpInfo());
      return CommandResult.SUCCESS;
   }

   static String getPasswordInteractively(ContextAwareCommandInvocation invocation) throws InterruptedException {
      String password = null;
      while (password == null || password.isEmpty()) {
         password = invocation.getShell().readLine(new Prompt(MSG.userToolPassword(), '*'));
      }
      String confirm = null;
      while (confirm == null || !confirm.equals(password)) {
         confirm = invocation.getShell().readLine(new Prompt(MSG.userToolPasswordConfirm(), '*'));
      }
      return password;
   }

   @CommandDefinition(name = Create.CMD, description = "Creates a user", aliases = "add")
   public static class Create extends CliCommand {
      public static final String CMD = "create";

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
               password = getPasswordInteractively(invocation);
            } catch (InterruptedException e) {
               return CommandResult.FAILURE;
            }
         }
         userTool.createUser(username, password, realm, UserTool.Encryption.valueOf(plainText), groups, algorithms);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = Describe.CMD, description = "Describes a user")
   public static class Describe extends CliCommand {
      public static final String CMD = "describe";

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
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         invocation.getShell().writeln(userTool.describeUser(username));
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = Remove.CMD, description = "Removes a user", aliases = "rm")
   public static class Remove extends CliCommand {
      public static final String CMD = "remove";

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
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.removeUser(username);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = Password.CMD, description = "Changes a user's password")
   public static class Password extends CliCommand {
      public static final String CMD = "password";

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
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         if (password == null) { // Get the password interactively
            try {
               password = getPasswordInteractively(invocation);
            } catch (InterruptedException e) {
               return CommandResult.FAILURE;
            }
         }
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.modifyUser(username, password, realm, UserTool.Encryption.valueOf(plainText), null, algorithms);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = Groups.CMD, description = "Sets a user's groups")
   public static class Groups extends CliCommand {
      public static final String CMD = "groups";

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
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.modifyUser(username, null, null, UserTool.Encryption.DEFAULT, groups, null);
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = Ls.CMD, description = "Lists all users/groups")
   public static class Ls extends CliCommand {
      public static final String CMD = "ls";

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
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);

         List<String> items;
         if (groups) {
            items = userTool.listGroups();
         } else {
            items = userTool.listUsers();
         }
         ObjectWriter json = new ObjectMapper().writerWithDefaultPrettyPrinter();
         try {
            invocation.getShell().writeln(json.writeValueAsString(items));
            return CommandResult.SUCCESS;
         } catch (JsonProcessingException e) {
            return CommandResult.FAILURE;
         }
      }
   }

   @CommandDefinition(name = Encrypt.CMD, description = "Encrypts all of the passwords in a property file.")
   public static class Encrypt extends CliCommand {
      public static final String CMD = "encrypt-all";

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
      public CommandResult exec(ContextAwareCommandInvocation invocation) {
         UserTool userTool = new UserTool(serverRoot, usersFile, groupsFile);
         userTool.encryptAll(algorithms);
         return CommandResult.SUCCESS;
      }
   }
}
