package org.infinispan.cli.commands;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Argument;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.logging.Messages;
import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MetaInfServices(Command.class)
@CommandDefinition(name = "connect", description = "Connects to a remote server")
public class Connect extends CliCommand {
   @Argument(description = "The connection string 'http://<host>:<port>")
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

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      try {
         CLI.configureSslContext(invocation.getContext(), truststore, truststorePassword, keystore, keystorePassword, provider, hostnameVerifier, trustAll);
      } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException | IOException e) {
         invocation.getShell().writeln(Messages.MSG.keyStoreError(e));
         throw new RuntimeException(e);
      }
      if (username != null) {
         invocation.getContext().connect(invocation.getShell(), connectionString, username, password);
      } else {
         invocation.getContext().connect(invocation.getShell(), connectionString);
      }
      return invocation.getContext().isConnected() ? CommandResult.SUCCESS : CommandResult.FAILURE;
   }
}
