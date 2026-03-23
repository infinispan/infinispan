package org.infinispan.cli.commands;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.FileResource;
import org.aesh.io.Resource;
import org.infinispan.cli.completers.BookmarkCompleter;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "connect", description = "Connects to a remote server")
public class Connect extends CliCommand {
   @Argument(description = "The connection string 'http://<host>:<port>' or a bookmark name", completer = BookmarkCompleter.class)
   String connectionString;

   @Option(shortName = 'u')
   String username;

   @Option(shortName = 'p')
   String password;

   @Option(completer = FileOptionCompleter.class, shortName = 't', name = "truststore", description = "A truststore to use when connecting to SSL/TLS-enabled servers")
   Resource truststore;

   @Option(shortName = 's', name = "truststore-password", description = "The password for the truststore")
   String truststorePassword;

   @Option(completer = FileOptionCompleter.class, shortName = 'k', name = "keystore", description = "A keystore containing a client certificate to authenticate with the server")
   Resource keystore;

   @Option(shortName = 'w', name = "keystore-password", description = "The password for the keystore")
   String keystorePassword;

   @Option(name = "provider", description = "The security provider used to create the SSL/TLS context")
   String provider;

   @Option(hasValue = false, description = "Whether to trust all server certificates", name = "trustall")
   boolean trustAll;

   @Option(name = "hostname-verifier", description = "A regular expression used to match hostnames when connecting to SSL/TLS-enabled servers")
   String hostnameVerifier;

   @Option(name = "context-path", description = "The context path for the server REST connector. If unspecified, defaults to /rest")
   String contextPath;


   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      String resolvedUrl = connectionString;
      String resolvedUsername = username;
      String resolvedPassword = password;
      Resource resolvedTruststore = truststore;
      String resolvedTruststorePassword = truststorePassword;
      Resource resolvedKeystore = keystore;
      String resolvedKeystorePassword = keystorePassword;
      boolean resolvedTrustAll = trustAll;
      String resolvedHostnameVerifier = hostnameVerifier;

      // If the connection string doesn't look like a URL, try to resolve it as a bookmark
      if (connectionString != null && !connectionString.contains("://")) {
         Bookmark.ResolvedBookmark bookmark = Bookmark.resolve(invocation, connectionString);
         if (bookmark != null) {
            resolvedUrl = bookmark.url();
            if (resolvedUsername == null) {
               resolvedUsername = bookmark.username();
            }
            if (resolvedPassword == null) {
               resolvedPassword = bookmark.password();
            }
            if (resolvedTruststore == null && bookmark.truststore() != null) {
               resolvedTruststore = new FileResource(Paths.get(bookmark.truststore()).toFile());
            }
            if (resolvedTruststorePassword == null) {
               resolvedTruststorePassword = bookmark.truststorePassword();
            }
            if (resolvedKeystore == null && bookmark.keystore() != null) {
               resolvedKeystore = new FileResource(Paths.get(bookmark.keystore()).toFile());
            }
            if (resolvedKeystorePassword == null) {
               resolvedKeystorePassword = bookmark.keystorePassword();
            }
            if (!resolvedTrustAll) {
               resolvedTrustAll = bookmark.trustAll();
            }
            if (resolvedHostnameVerifier == null) {
               resolvedHostnameVerifier = bookmark.hostnameVerifier();
            }
         }
      }

      // Validate the protocol if the URL contains a scheme
      if (resolvedUrl != null && resolvedUrl.contains("://")) {
         String scheme = URI.create(resolvedUrl).getScheme();
         if (scheme != null && !"http".equals(scheme) && !"https".equals(scheme)) {
            throw Messages.MSG.unsupportedProtocol(scheme);
         }
      }

      try {
         CLI.configureSslContext(invocation.getContext(), resolvedTruststore, resolvedTruststorePassword, resolvedKeystore, resolvedKeystorePassword, provider, resolvedHostnameVerifier, resolvedTrustAll);
      } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException | IOException e) {
         invocation.getShell().writeln(Messages.MSG.keyStoreError(e));
         throw new RuntimeException(e);
      }
      if (contextPath != null) {
         invocation.getContext().setProperty(RestClientConfigurationProperties.CONTEXT_PATH, contextPath);
      }
      if (resolvedUsername != null) {
         invocation.getContext().connect(invocation.getShell(), resolvedUrl, resolvedUsername, resolvedPassword);
      } else {
         invocation.getContext().connect(invocation.getShell(), resolvedUrl);
      }
      return invocation.getContext().isConnected() ? CommandResult.SUCCESS : CommandResult.FAILURE;
   }
}
