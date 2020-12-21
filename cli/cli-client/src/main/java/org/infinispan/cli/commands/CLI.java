package org.infinispan.cli.commands;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.aesh.AeshRuntimeRunner;
import org.aesh.command.AeshCommandRuntimeBuilder;
import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.GroupCommandDefinition;
import org.aesh.command.impl.completer.FileOptionCompleter;
import org.aesh.command.impl.registry.AeshCommandRegistryBuilder;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.option.Option;
import org.aesh.command.option.OptionGroup;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.registry.CommandRegistryException;
import org.aesh.command.settings.SettingsBuilder;
import org.aesh.command.shell.Shell;
import org.aesh.io.Resource;
import org.aesh.readline.ReadlineConsole;
import org.infinispan.cli.Context;
import org.infinispan.cli.activators.ContextAwareCommandActivatorProvider;
import org.infinispan.cli.completers.ContextAwareCompleterInvocationProvider;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.cli.impl.CliCommandNotFoundHandler;
import org.infinispan.cli.impl.CliRuntimeRunner;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.ContextAwareCommandInvocationProvider;
import org.infinispan.cli.impl.ContextAwareQuitHandler;
import org.infinispan.cli.impl.ContextImpl;
import org.infinispan.cli.impl.DefaultShell;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.util.ZeroSecurityHostnameVerifier;
import org.infinispan.cli.util.ZeroSecurityTrustManager;
import org.infinispan.commons.util.ServiceFinder;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.provider.util.ProviderUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@GroupCommandDefinition(
      name = CLI.CMD,
      description = "",
      groupCommands = {
            Add.class,
            Cache.class,
            Cas.class,
            Cd.class,
            Clear.class,
            ClearCache.class,
            Config.class,
            Connect.class,
            Container.class,
            Counter.class,
            Create.class,
            Describe.class,
            Disconnect.class,
            Drop.class,
            Echo.class,
            Encoding.class,
            Get.class,
            Ls.class,
            Patch.class,
            Put.class,
            Query.class,
            Remove.class,
            Replace.class,
            Reset.class,
            Run.class,
            Schema.class,
            Shutdown.class,
            Site.class,
            User.class,
            Version.class
      })
public class CLI extends CliCommand {
   public static final String CMD = "cli";

   private Context context;

   @Option(completer = FileOptionCompleter.class, shortName = 't', name = "truststore", description = "A truststore to use when connecting to SSL/TLS-enabled servers")
   Resource truststore;

   @Option(shortName = 's', name = "truststore-password", description = "The password for the truststore")
   String truststorePassword;

   @Option(shortName = 'v', hasValue = false, description = "Shows version information")
   boolean version;

   @Option(hasValue = false, description = "Whether to trust all certificates", name = "trustall")
   boolean trustAll;

   @Option(completer = FileOptionCompleter.class, shortName = 'f', description = "File for batch mode")
   Resource file;

   @Option(shortName = 'c', description = "A connection URL. Use '-' to connect to http://localhost:11222")
   String connect;

   @Option(shortName = 'P', description = "Sets system properties from the specified file.")
   String properties;

   @OptionGroup(shortName = 'D', description = "Sets a system property")
   Map<String, String> propertyMap;

   @Option(shortName = 'h', hasValue = false, overrideRequired = true)
   protected boolean help;

   @Override
   public boolean isHelp() {
      return help;
   }

   @Override
   public CommandResult exec(ContextAwareCommandInvocation invocation) {
      if (help) {
         invocation.println(invocation.getHelpInfo());
         return CommandResult.SUCCESS;
      }
      if (version) {
         invocation.printf("%s CLI %s\n", org.infinispan.commons.util.Version.getBrandName(), org.infinispan.commons.util.Version.getBrandVersion());
         invocation.printf("Copyright (C) Red Hat Inc. and/or its affiliates and other contributors\n");
         invocation.printf("License Apache License, v. 2.0. http://www.apache.org/licenses/LICENSE-2.0\n");
         return CommandResult.SUCCESS;
      }

      context = invocation.getContext();

      if (propertyMap != null) {
         propertyMap.forEach(context.getProperties()::putIfAbsent);
      }

      if (properties != null) {
         try(Reader r = Files.newBufferedReader(Paths.get(properties))) {
            Properties loaded = new Properties();
            loaded.load(r);
            loaded.forEach(context.getProperties()::putIfAbsent);
         } catch (IOException e) {
            throw new IllegalArgumentException(e);
         }
      }

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

      if (file != null) {
         return batch(file.getAbsolutePath(), invocation.getShell());
      } else {
         if (context.getProperty(Context.Property.AUTOEXEC) != null) {
            batch(context.getProperty(Context.Property.AUTOEXEC), invocation.getShell());
         }
         return interactive(invocation.getShell());
      }
   }

   private CommandResult batch(String inputFile, Shell shell) {
      CommandRegistry commandRegistry = initializeCommands(Batch.class);

      AeshCommandRuntimeBuilder runtimeBuilder = AeshCommandRuntimeBuilder.builder();
      runtimeBuilder
            .commandActivatorProvider(new ContextAwareCommandActivatorProvider(context))
            .commandInvocationProvider(new ContextAwareCommandInvocationProvider(context))
            .commandNotFoundHandler(new CliCommandNotFoundHandler())
            .completerInvocationProvider(new ContextAwareCompleterInvocationProvider(context))
            .aeshContext(context)
            .commandRegistry(commandRegistry);

      runtimeBuilder.shell(shell);

      CliRuntimeRunner cliRunner = CliRuntimeRunner.builder("batch", runtimeBuilder.build());
      cliRunner
            .args(new String[]{"run", inputFile})
            .execute();
      context.disconnect();
      return CommandResult.SUCCESS;
   }

   private CommandResult interactive(Shell shell) {
      // We now start an interactive CLI
      CommandRegistry commandRegistry = initializeCommands();
      context.setRegistry(commandRegistry);
      SettingsBuilder settings = SettingsBuilder.builder();
      settings
            .enableAlias(true)
            .aliasFile(context.getConfigPath().resolve("aliases").toFile())
            .historyFile(context.getConfigPath().resolve("history").toFile())
            .outputStream(System.out)
            .outputStreamError(System.err)
            .inputStream(System.in)
            .commandActivatorProvider(new ContextAwareCommandActivatorProvider(context))
            .commandInvocationProvider(new ContextAwareCommandInvocationProvider(context))
            .commandNotFoundHandler(new CliCommandNotFoundHandler())
            .completerInvocationProvider(new ContextAwareCompleterInvocationProvider(context))
            .commandRegistry(commandRegistry)
            .aeshContext(context)
            .quitHandler(new ContextAwareQuitHandler(context));

      if (shell instanceof AeshDelegatingShell) {
         settings.connection(((AeshDelegatingShell)shell).getConnection());
      }

      ReadlineConsole console = new ReadlineConsole(settings.build());
      context.setConsole(console);
      try {
         console.start();
         return CommandResult.SUCCESS;
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private CommandRegistry initializeCommands(Class<? extends Command>... commands) {
      try {
         AeshCommandRegistryBuilder<CommandInvocation> registryBuilder = AeshCommandRegistryBuilder.builder();
         for (Class<? extends Command> command : commands) {
            registryBuilder.command(command);
         }
         for (Command command : ServiceFinder.load(Command.class, this.getClass().getClassLoader())) {
            registryBuilder.command(command);
         }
         return registryBuilder.create();
      } catch (CommandRegistryException e) {
         throw new RuntimeException(e);
      }
   }

   private static AeshCommandRuntimeBuilder initialCommandRuntimeBuilder(Shell shell, Properties properties) throws CommandRegistryException {
      AeshCommandRegistryBuilder registryBuilder = AeshCommandRegistryBuilder.builder().command(CLI.class);
      Context context = new ContextImpl(properties);
      AeshCommandRuntimeBuilder runtimeBuilder = AeshCommandRuntimeBuilder.builder();
      runtimeBuilder
            .commandActivatorProvider(new ContextAwareCommandActivatorProvider(context))
            .commandInvocationProvider(new ContextAwareCommandInvocationProvider(context))
            .commandNotFoundHandler(new CliCommandNotFoundHandler())
            .completerInvocationProvider(new ContextAwareCompleterInvocationProvider(context))
            .shell(shell)
            .aeshContext(context)
            .commandRegistry(registryBuilder.create());
      return runtimeBuilder;
   }

   public static void main(Shell shell, String[] args, Properties properties) {
      try {
         AeshCommandRuntimeBuilder runtimeBuilder = initialCommandRuntimeBuilder(shell, properties);
         AeshRuntimeRunner.builder().commandRuntime(runtimeBuilder.build()).args(args).execute();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public static void main(String[] args) {
      try {
         AeshCommandRuntimeBuilder runtimeBuilder = initialCommandRuntimeBuilder(new DefaultShell(), System.getProperties());
         AeshRuntimeRunner.builder().commandRuntime(runtimeBuilder.build()).args(args).execute();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
