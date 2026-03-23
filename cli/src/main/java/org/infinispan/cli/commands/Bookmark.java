package org.infinispan.cli.commands;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.infinispan.cli.Context;
import org.infinispan.cli.completers.BookmarkCompleter;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.store.CredentialStoreException;
import org.wildfly.security.credential.store.impl.KeyStoreCredentialStore;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @since 16.2
 */
@MetaInfServices(Command.class)
@GroupCommandDefinition(name = "bookmark", description = "Manage connection bookmarks",
      groupCommands = {Bookmark.SetBookmark.class, Bookmark.Remove.class, Bookmark.Ls.class, Bookmark.Get.class})
public class Bookmark extends CliCommand {

   public static final String BOOKMARKS_FILE = "bookmarks.properties";
   static final String CREDENTIAL_STORE_FILE = "bookmarks.pfx";
   private static final Set<String> SECRET_KEYS = Set.of("password", "truststore-password", "keystore-password");

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      invocation.println(invocation.getHelpInfo());
      return CommandResult.FAILURE;
   }

   @CommandDefinition(name = "set", description = "Creates or updates a bookmark")
   public static class SetBookmark extends CliCommand {

      @Argument(description = "The bookmark name", required = true, completer = BookmarkCompleter.class)
      String name;

      @Option(shortName = 'u', description = "The connection URL (e.g. hotrod://host:11222, http://host:11222). If omitted and the CLI is connected, the current connection URL is used.")
      String url;

      @Option(description = "The username for authentication. If unspecified, the ISPN_USERNAME environment variable will be used.", defaultValue = "${env:ISPN_USERNAME}")
      String username;

      @Option(description = "The password for authentication. If unspecified, the ISPN_PASSWORD environment variable will be used.", defaultValue = "${env:ISPN_PASSWORD}")
      String password;

      @Option(description = "The path to a truststore file", completer = FileOptionCompleter.class)
      String truststore;

      @Option(name = "truststore-password", description = "The password for the truststore")
      String truststorePassword;

      @Option(description = "The path to a keystore file", completer = FileOptionCompleter.class)
      String keystore;

      @Option(name = "keystore-password", description = "The password for the keystore")
      String keystorePassword;

      @Option(hasValue = false, description = "Whether to trust all server certificates", name = "trustall")
      boolean trustAll;

      @Option(name = "hostname-verifier", description = "A regular expression used to match hostnames when connecting to SSL/TLS-enabled servers")
      String hostnameVerifier;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         // Infer values from the current connection if not explicitly provided
         if (invocation.getContext().isConnected()) {
            Connection connection = invocation.getContext().connection();
            if (url == null) {
               url = connection.getURI();
            }
            if (username == null) {
               username = connection.getUsername();
            }
            Context context = invocation.getContext();
            if (truststore == null) {
               truststore = context.getProperty(Context.Property.TRUSTSTORE);
            }
            if (truststorePassword == null) {
               truststorePassword = context.getProperty(Context.Property.TRUSTSTORE_PASSWORD);
            }
            if (keystore == null) {
               keystore = context.getProperty(Context.Property.KEYSTORE);
            }
            if (keystorePassword == null) {
               keystorePassword = context.getProperty(Context.Property.KEYSTORE_PASSWORD);
            }
         }

         // For new bookmarks, a URL is required. For existing ones, keep the current URL.
         Properties existing = loadBookmarks(invocation);
         if (url == null) {
            url = existing.getProperty(name + ".url");
         }
         if (url == null) {
            throw Messages.MSG.bookmarkUrlRequired();
         }

         // Extract credentials from URL userInfo and store them separately
         URI parsedUri = URI.create(url);
         String userInfo = parsedUri.getUserInfo();
         if (userInfo != null) {
            int colon = userInfo.indexOf(':');
            if (colon >= 0) {
               if (username == null) {
                  username = userInfo.substring(0, colon);
               }
               if (password == null) {
                  password = userInfo.substring(colon + 1);
               }
            } else {
               if (username == null) {
                  username = userInfo;
               }
            }
            // Rebuild URL without credentials
            StringBuilder cleanUrl = new StringBuilder();
            cleanUrl.append(parsedUri.getScheme()).append("://");
            cleanUrl.append(parsedUri.getHost());
            if (parsedUri.getPort() >= 0) {
               cleanUrl.append(':').append(parsedUri.getPort());
            }
            if (parsedUri.getPath() != null && !parsedUri.getPath().isEmpty()) {
               cleanUrl.append(parsedUri.getPath());
            }
            if (parsedUri.getQuery() != null) {
               cleanUrl.append('?').append(parsedUri.getQuery());
            }
            if (parsedUri.getFragment() != null) {
               cleanUrl.append('#').append(parsedUri.getFragment());
            }
            url = cleanUrl.toString();
         }

         Properties bookmarks = loadBookmarks(invocation);
         setIfPresent(bookmarks, name, "url", url);
         setIfPresent(bookmarks, name, "username", username);
         setIfPresent(bookmarks, name, "truststore", truststore);
         setIfPresent(bookmarks, name, "keystore", keystore);
         if (trustAll) {
            bookmarks.setProperty(name + ".trustall", "true");
         }
         setIfPresent(bookmarks, name, "hostname-verifier", hostnameVerifier);
         saveBookmarks(invocation, bookmarks);

         // Store secrets in the credential store
         boolean hasSecrets = password != null || truststorePassword != null || keystorePassword != null;
         if (hasSecrets) {
            try {
               KeyStoreCredentialStore store = openCredentialStoreWithFallback(invocation, true);
               storeSecret(store, name, "password", password);
               storeSecret(store, name, "truststore-password", truststorePassword);
               storeSecret(store, name, "keystore-password", keystorePassword);
               store.flush();
               restrictFilePermissions(invocation);
            } catch (CredentialStoreException e) {
               throw Messages.MSG.failedToStoreCredentials(e);
            }
         }

         invocation.println(Messages.MSG.bookmarkSaved(name));
         return CommandResult.SUCCESS;
      }

      private void setIfPresent(Properties props, String bookmark, String key, String value) {
         String propKey = bookmark + "." + key;
         if (value != null) {
            props.setProperty(propKey, value);
         }
      }
   }

   @CommandDefinition(name = "remove", description = "Removes a bookmark", aliases = "rm")
   public static class Remove extends CliCommand {

      @Argument(description = "The bookmark name to remove", required = true, completer = BookmarkCompleter.class)
      String name;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         Properties bookmarks = loadBookmarks(invocation);
         String prefix = name + ".";
         List<String> toRemove = bookmarks.stringPropertyNames().stream()
               .filter(k -> k.startsWith(prefix)).toList();
         boolean found = !toRemove.isEmpty();
         toRemove.forEach(bookmarks::remove);
         if (!found) {
            invocation.errorln(Messages.MSG.bookmarkNotFound(name));
            return CommandResult.FAILURE;
         }
         saveBookmarks(invocation, bookmarks);

         // Remove secrets from credential store if it exists
         Path storePath = invocation.getContext().configPath().resolve(CREDENTIAL_STORE_FILE);
         if (Files.exists(storePath)) {
            try {
               KeyStoreCredentialStore store = openCredentialStoreWithFallback(invocation, false);
               for (String secretKey : SECRET_KEYS) {
                  String alias = credentialAlias(name, secretKey);
                  if (store.exists(alias, PasswordCredential.class)) {
                     store.remove(alias, PasswordCredential.class, null, null);
                  }
               }
               store.flush();
            } catch (CredentialStoreException e) {
               invocation.errorln(Messages.MSG.cannotRemoveCredentials(e.getMessage()));
            }
         }

         invocation.println(Messages.MSG.bookmarkRemoved(name));
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "ls", description = "Lists all bookmarks")
   public static class Ls extends CliCommand {

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         Properties bookmarks = loadBookmarks(invocation);
         Set<String> names = new TreeSet<>();
         for (String key : bookmarks.stringPropertyNames()) {
            int dot = key.indexOf('.');
            if (dot > 0) {
               names.add(key.substring(0, dot));
            }
         }
         if (names.isEmpty()) {
            invocation.println(Messages.MSG.noBookmarks());
         } else {
            for (String name : names) {
               String url = bookmarks.getProperty(name + ".url", "");
               String username = bookmarks.getProperty(name + ".username");
               if (username != null) {
                  invocation.println(name + " = " + url + " (user: " + username + ")");
               } else {
                  invocation.println(name + " = " + url);
               }
            }
         }
         return CommandResult.SUCCESS;
      }
   }

   @CommandDefinition(name = "get", description = "Shows details for a bookmark")
   public static class Get extends CliCommand {

      @Argument(description = "The bookmark name", required = true, completer = BookmarkCompleter.class)
      String name;

      @Override
      public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
         Properties bookmarks = loadBookmarks(invocation);
         String prefix = name + ".";
         boolean found = false;
         for (String key : new TreeSet<>(bookmarks.stringPropertyNames())) {
            if (key.startsWith(prefix)) {
               String prop = key.substring(prefix.length());
               invocation.println(prop + " = " + bookmarks.getProperty(key));
               found = true;
            }
         }

         // Show which secrets are stored (without revealing values)
         Path storePath = invocation.getContext().configPath().resolve(CREDENTIAL_STORE_FILE);
         if (Files.exists(storePath)) {
            try {
               KeyStoreCredentialStore store = openCredentialStoreWithFallback(invocation, false);
               for (String secretKey : SECRET_KEYS) {
                  if (store.exists(credentialAlias(name, secretKey), PasswordCredential.class)) {
                     invocation.println(secretKey + " = ********");
                     found = true;
                  }
               }
            } catch (CredentialStoreException e) {
               invocation.errorln(Messages.MSG.cannotReadCredentials(e.getMessage()));
            }
         }

         if (!found) {
            invocation.errorln(Messages.MSG.bookmarkNotFound(name));
            return CommandResult.FAILURE;
         }
         return CommandResult.SUCCESS;
      }
   }

   static Properties loadBookmarks(ContextAwareCommandInvocation invocation) throws CommandException {
      Path configPath = invocation.getContext().configPath();
      Path bookmarksFile = configPath.resolve(BOOKMARKS_FILE);
      Properties props = new Properties();
      if (Files.exists(bookmarksFile)) {
         try (Reader r = Files.newBufferedReader(bookmarksFile)) {
            props.load(r);
         } catch (IOException e) {
            throw Messages.MSG.bookmarkLoadFailure(e);
         }
      }
      return props;
   }

   static void saveBookmarks(ContextAwareCommandInvocation invocation, Properties bookmarks) throws CommandException {
      Path configPath = invocation.getContext().configPath();
      Path bookmarksFile = configPath.resolve(BOOKMARKS_FILE);
      try {
         Files.createDirectories(configPath);
         try (Writer w = Files.newBufferedWriter(bookmarksFile)) {
            bookmarks.store(w, null);
         }
      } catch (IOException e) {
         throw Messages.MSG.bookmarkSaveFailure(e);
      }
   }

   static String credentialAlias(String bookmarkName, String secretKey) {
      return "bookmark." + bookmarkName + "." + secretKey;
   }

   static KeyStoreCredentialStore openCredentialStore(ContextAwareCommandInvocation invocation, boolean create, char[] masterPassword)
         throws CredentialStoreException {
      Path storePath = invocation.getContext().configPath().resolve(CREDENTIAL_STORE_FILE);
      return Credentials.getKeyStoreCredentialStore(storePath, Credentials.STORE_TYPE, create, masterPassword);
   }

   /**
    * Attempts to open the credential store with an empty password first.
    * If that fails, prompts the user for the master password.
    */
   static KeyStoreCredentialStore openCredentialStoreWithFallback(ContextAwareCommandInvocation invocation, boolean create)
         throws CredentialStoreException, CommandException {
      try {
         return openCredentialStore(invocation, false, new char[0]);
      } catch (CredentialStoreException e) {
         char[] masterPassword = getMasterPassword(invocation);
         return openCredentialStore(invocation, create, masterPassword);
      }
   }

   static void restrictFilePermissions(ContextAwareCommandInvocation invocation) {
      Path storePath = invocation.getContext().configPath().resolve(CREDENTIAL_STORE_FILE);
      try {
         Files.setPosixFilePermissions(storePath, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
      } catch (UnsupportedOperationException e) {
         // Non-POSIX filesystem (e.g. Windows), skip
      } catch (IOException e) {
         invocation.errorln("Warning: could not restrict permissions on " + storePath + ": " + e.getMessage());
      }
   }

   static char[] getMasterPassword(ContextAwareCommandInvocation invocation) throws CommandException {
      try {
         return invocation.getPasswordInteractively(Messages.MSG.bookmarkMasterPassword(), null).toCharArray();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CommandException("Interrupted while reading master password", e);
      }
   }

   private static void storeSecret(KeyStoreCredentialStore store, String bookmarkName, String secretKey, String value)
         throws CredentialStoreException {
      if (value != null) {
         String alias = credentialAlias(bookmarkName, secretKey);
         store.store(alias, new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, value.toCharArray())), null);
      }
   }

   /**
    * Resolved bookmark data including secrets.
    */
   public record ResolvedBookmark(String url, String username, String password,
                                  String truststore, String truststorePassword,
                                  String keystore, String keystorePassword,
                                  boolean trustAll, String hostnameVerifier) {
   }

   /**
    * Resolves a bookmark by name, loading properties and secrets.
    * Returns null if the bookmark does not exist.
    */
   public static ResolvedBookmark resolve(ContextAwareCommandInvocation invocation, String bookmarkName) throws CommandException {
      Properties bookmarks = loadBookmarks(invocation);
      String url = bookmarks.getProperty(bookmarkName + ".url");
      if (url == null) {
         return null;
      }
      String username = bookmarks.getProperty(bookmarkName + ".username");
      String truststore = bookmarks.getProperty(bookmarkName + ".truststore");
      String keystore = bookmarks.getProperty(bookmarkName + ".keystore");
      boolean trustAll = Boolean.parseBoolean(bookmarks.getProperty(bookmarkName + ".trustall"));
      String hostnameVerifier = bookmarks.getProperty(bookmarkName + ".hostname-verifier");

      String password = null;
      String truststorePassword = null;
      String keystorePassword = null;
      Path storePath = invocation.getContext().configPath().resolve(CREDENTIAL_STORE_FILE);
      if (Files.exists(storePath)) {
         try {
            KeyStoreCredentialStore store = openCredentialStoreWithFallback(invocation, false);
            password = retrieveSecret(store, bookmarkName, "password");
            truststorePassword = retrieveSecret(store, bookmarkName, "truststore-password");
            keystorePassword = retrieveSecret(store, bookmarkName, "keystore-password");
         } catch (CredentialStoreException e) {
            throw Messages.MSG.cannotReadCredentials(e);
         }
      }

      return new ResolvedBookmark(url, username, password, truststore, truststorePassword, keystore, keystorePassword, trustAll, hostnameVerifier);
   }

   private static String retrieveSecret(KeyStoreCredentialStore store, String bookmarkName, String secretKey)
         throws CredentialStoreException {
      String alias = credentialAlias(bookmarkName, secretKey);
      PasswordCredential credential = store.retrieve(alias, PasswordCredential.class, null, null, null);
      if (credential != null) {
         return new String(credential.getPassword().castAndApply(ClearPassword.class, ClearPassword::getPassword));
      }
      return null;
   }
}
