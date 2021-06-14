package org.infinispan.cli.commands;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.aesh.command.Command;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.option.Option;
import org.aesh.io.Resource;
import org.aesh.terminal.utils.ANSI;
import org.infinispan.cli.Context;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.util.ZeroSecurityHostnameVerifier;
import org.infinispan.cli.util.ZeroSecurityTrustManager;
import org.infinispan.commons.util.Util;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.provider.util.ProviderUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class CliCommand implements Command<ContextAwareCommandInvocation> {

   @Option(description = "Specifies the URL for the server connection. Use '-' to connect to http://localhost:11222", name = "connect")
   String connect;

   @Option(hasValue = false, description = "Trusts all certificates as valid server identities.", name = "trustall")
   boolean trustAll;

   @Option(completer = FileOptionCompleter.class, name = "truststore", description = "Specifies a trust store that contains SSL/TLS certificates to verify server identities.")
   Resource truststore;

   @Option(name = "truststore-password", description = "Specifies the password for the trust store.")
   String truststorePassword;

   @Override
   public CommandResult execute(ContextAwareCommandInvocation invocation) throws CommandException {
      if (isHelp()) {
         invocation.println(invocation.getHelpInfo());
         return CommandResult.SUCCESS;
      }

      Context context = invocation.getContext();

      String sslTrustStore = truststore != null ? truststore.getAbsolutePath() : context.getProperty(Context.Property.TRUSTSTORE);
      if (sslTrustStore != null) {
         String sslTrustStorePassword = truststorePassword != null ? truststorePassword : context.getProperty(Context.Property.TRUSTSTORE_PASSWORD);
         try (FileInputStream f = new FileInputStream(sslTrustStore)) {
            KeyStore keyStore = KeyStoreUtil.loadKeyStore(ProviderUtil.INSTALLED_PROVIDERS, null, f, sslTrustStore, sslTrustStorePassword != null ? sslTrustStorePassword.toCharArray() : null);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            SSLContextSettings sslContext = SSLContextSettings.getInstance("TLS", null, trustManagerFactory.getTrustManagers(), null, null);
            context.setSslContext(sslContext);
         } catch (Exception e) {
            invocation.getShell().writeln(MSG.keyStoreError(sslTrustStore, e));
            return CommandResult.FAILURE;
         }
      } else if (trustAll || Boolean.parseBoolean(context.getProperty(Context.Property.TRUSTALL))) {
         SSLContextSettings sslContext = SSLContextSettings.getInstance("TLS", null, new TrustManager[]{new ZeroSecurityTrustManager()}, null, new ZeroSecurityHostnameVerifier());
         context.setSslContext(sslContext);
      }

      String connectionString = connect != null ? connect : context.getProperty(Context.Property.AUTOCONNECT_URL);

      if (connectionString != null) {
         context.connect(null, connectionString);
      }
      try {
         return exec(invocation);
      } catch (CommandException e) {
         Throwable cause = Util.getRootCause(e);
         invocation.getShell().writeln(ANSI.RED_TEXT + e.getLocalizedMessage() + ANSI.DEFAULT_TEXT);
         if (cause != e) {
            invocation.getShell().writeln(ANSI.RED_TEXT + cause.getClass().getSimpleName() +": " + cause.getLocalizedMessage() + ANSI.DEFAULT_TEXT);
         }
         return CommandResult.FAILURE;
      } catch (Throwable e) {
         // These are unhandled
         Throwable cause = Util.getRootCause(e);
         invocation.getShell().writeln(ANSI.RED_TEXT + cause.getClass().getSimpleName() +": " + cause.getLocalizedMessage() + ANSI.DEFAULT_TEXT);
         return CommandResult.FAILURE;
      }
   }

   protected abstract boolean isHelp();

   protected abstract CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException;

   public int nesting() {
      return 0;
   }
}
