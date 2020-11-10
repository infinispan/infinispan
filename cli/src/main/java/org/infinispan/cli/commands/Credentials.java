package org.infinispan.cli.commands;

import static org.infinispan.cli.logging.Messages.MSG;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.FileResource;
import org.aesh.io.Resource;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStore;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = Credentials.CMD, description = "Credential store operations", groupCommands = {Credentials.Add.class, Credentials.Remove.class, Credentials.Ls.class})
public class Credentials extends CliCommand {

   public static final String CMD = "credentials";
   public static final String STORE_TYPE = "pkcs12";
   public static final String CREDENTIALS_PATH = "credentials.pfx";

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

   static KeyStoreCredentialStore getKeyStoreCredentialStore(Path path, String type, boolean create, char[] password) throws CredentialStoreException {
      KeyStoreCredentialStore store = new KeyStoreCredentialStore();
      final Map<String, String> map = new HashMap<>();
      map.put("location", path.toAbsolutePath().toString());
      map.put("keyStoreType", type);
      map.put("create", Boolean.toString(create));
      store.initialize(
            map,
            new CredentialStore.CredentialSourceProtectionParameter(
                  IdentityCredentials.NONE.withCredential(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, password)))),
            null
      );
      return store;
   }

   static Path resourceToPath(Resource resource, String serverRoot) {
      if (((FileResource) resource).getFile().getParent() != null) {
         return Paths.get(resource.getAbsolutePath());
      } else {
         String serverHome = System.getProperty("infinispan.server.home.path");
         Path serverHomePath = serverHome == null ? Paths.get("") : Paths.get(serverHome);
         return serverHomePath.resolve(serverRoot).resolve("conf").resolve(((FileResource) resource).getFile().getPath()).toAbsolutePath();
      }

   }

   @CommandDefinition(name = Add.CMD, description = "Adds credentials to keystores.")
   public static class Add extends CliCommand {
      public static final String CMD = "add";

      @Argument(description = "Specifies an alias, or name, for the credential.", required = true)
      String alias;

      @Option(description = "Sets the path to a credential keystore and creates a new one if it does not exist.", completer = FileOptionCompleter.class, defaultValue = CREDENTIALS_PATH)
      Resource path;

      @Option(description = "Specifies a password to protect the credential keystore.", shortName = 'p')
      String password;

      @Option(description = "Sets the type of credential store. Values are either PKCS12, which is the default, or JCEKS.", shortName = 't', defaultValue = STORE_TYPE)
      String type;

      @Option(description = "Adds a credential to the keystore.", shortName = 'c')
      String credential;

      @Option(description = "Sets the path to the server root directory.", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Path file = resourceToPath(path, serverRoot);
            if (password == null) {
               password = invocation.getPasswordInteractively(MSG.credentialToolPassword(), Files.exists(file) ? null : MSG.credentialToolPasswordConfirm());
            }
            if (credential == null) {
               credential = invocation.getPasswordInteractively(MSG.credentialToolCredential(), MSG.credentialToolCredentialConfirm());
            }
            KeyStoreCredentialStore store = getKeyStoreCredentialStore(file, type, true, password.toCharArray());
            store.store(alias, new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, credential.toCharArray())), null);
            store.flush();
            return CommandResult.SUCCESS;
         } catch (Exception e) {
            throw new CommandException(e);
         }
      }
   }

   @CommandDefinition(name = Remove.CMD, description = "Deletes credentials from keystores.", aliases = "rm")
   public static class Remove extends CliCommand {
      public static final String CMD = "remove";

      @Argument(description = "Specifies an alias, or name, for the credential.", required = true)
      String alias;

      @Option(description = "Sets the path to a credential keystore.", completer = FileOptionCompleter.class, defaultValue = CREDENTIALS_PATH)
      Resource path;

      @Option(description = "Specifies the password that protects the credential keystore.", shortName = 'p')
      String password;

      @Option(description = "Sets the type of credential store. Values are either PKCS12, which is the default, or JCEKS.", shortName = 't', defaultValue = STORE_TYPE)
      String type;

      @Option(description = "Sets the path to the server root directory.", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Path file = resourceToPath(path, serverRoot);
            if (password == null) {
               password = invocation.getPasswordInteractively(MSG.credentialToolPassword(), null);
            }
            KeyStoreCredentialStore store = getKeyStoreCredentialStore(file, type, false, password.toCharArray());
            store.remove(alias, PasswordCredential.class, null, null);
            store.flush();
            return CommandResult.SUCCESS;
         } catch (Exception e) {
            throw new CommandException(e);
         }
      }
   }

   @CommandDefinition(name = Ls.CMD, description = "Lists credential aliases in keystores.")
   public static class Ls extends CliCommand {
      public static final String CMD = "ls";

      @Option(description = "Sets the path to a credential keystore.", completer = FileOptionCompleter.class, defaultValue = CREDENTIALS_PATH)
      Resource path;

      @Option(description = "Specifies the password that protects the credential keystore.", shortName = 'p')
      String password;

      @Option(description = "Sets the type of credential store. Values are either PKCS12, which is the default, or JCEKS.", shortName = 't', defaultValue = STORE_TYPE)
      String type;

      @Option(description = "Sets the path to the server root directory.", defaultValue = "server", name = "server-root", shortName = 's')
      String serverRoot;

      @Option(shortName = 'h', hasValue = false, overrideRequired = true)
      protected boolean help;

      @Override
      public boolean isHelp() {
         return help;
      }

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         try {
            Path file = resourceToPath(path, serverRoot);
            if (Files.exists(file)) {
               if (password == null) {
                  password = invocation.getPasswordInteractively(MSG.credentialToolPassword(), null);
               }
               KeyStoreCredentialStore store = getKeyStoreCredentialStore(file, type, false, password.toCharArray());
               for (String alias : store.getAliases()) {
                  invocation.println(alias);
               }
            }
            return CommandResult.SUCCESS;
         } catch (Exception e) {
            throw new CommandException(e);
         }
      }
   }
}
