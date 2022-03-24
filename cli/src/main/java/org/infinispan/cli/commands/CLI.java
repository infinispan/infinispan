package org.infinispan.cli.commands;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.aesh.command.AeshCommandRuntimeBuilder;
import org.aesh.command.Command;
import org.aesh.command.CommandResult;
import org.aesh.command.CommandRuntime;
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
import org.infinispan.cli.commands.kubernetes.Kube;
import org.infinispan.cli.commands.rest.Add;
import org.infinispan.cli.commands.rest.Alter;
import org.infinispan.cli.commands.rest.Availability;
import org.infinispan.cli.commands.rest.Backup;
import org.infinispan.cli.commands.rest.Cas;
import org.infinispan.cli.commands.rest.ClearCache;
import org.infinispan.cli.commands.rest.Create;
import org.infinispan.cli.commands.rest.Drop;
import org.infinispan.cli.commands.rest.Get;
import org.infinispan.cli.commands.rest.Index;
import org.infinispan.cli.commands.rest.Logging;
import org.infinispan.cli.commands.rest.Migrate;
import org.infinispan.cli.commands.rest.Put;
import org.infinispan.cli.commands.rest.Query;
import org.infinispan.cli.commands.rest.Rebalance;
import org.infinispan.cli.commands.rest.Remove;
import org.infinispan.cli.commands.rest.Reset;
import org.infinispan.cli.commands.rest.Schema;
import org.infinispan.cli.commands.rest.Server;
import org.infinispan.cli.commands.rest.Shutdown;
import org.infinispan.cli.commands.rest.Site;
import org.infinispan.cli.commands.rest.Task;
import org.infinispan.cli.completers.ContextAwareCompleterInvocationProvider;
import org.infinispan.cli.connection.RegexHostnameVerifier;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.cli.impl.CliAliasManager;
import org.infinispan.cli.impl.CliCommandNotFoundHandler;
import org.infinispan.cli.impl.CliRuntimeRunner;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.ContextAwareCommandInvocationProvider;
import org.infinispan.cli.impl.ContextAwareQuitHandler;
import org.infinispan.cli.impl.ContextImpl;
import org.infinispan.cli.impl.DefaultShell;
import org.infinispan.cli.impl.ExitCodeResultHandler;
import org.infinispan.cli.impl.KubernetesContext;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.util.ZeroSecurityHostnameVerifier;
import org.infinispan.cli.util.ZeroSecurityTrustManager;
import org.infinispan.commons.jdkspecific.ProcessInfo;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;
import org.wildfly.security.credential.store.WildFlyElytronCredentialStoreProvider;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.provider.util.ProviderUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@GroupCommandDefinition(
      name = "cli",
      description = "",
      groupCommands = {
            Add.class,
            Alter.class,
            Availability.class,
            Backup.class,
            Benchmark.class,
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
            Credentials.class,
            Describe.class,
            Disconnect.class,
            Drop.class,
            Echo.class,
            Encoding.class,
            Get.class,
            Index.class,
            Install.class,
            Logging.class,
            Ls.class,
            Migrate.class,
            Patch.class,
            Put.class,
            Query.class,
            Rebalance.class,
            Remove.class,
            Reset.class,
            Run.class,
            Schema.class,
            Server.class,
            Shutdown.class,
            Site.class,
            Task.class,
            User.class,
            Version.class
      }, resultHandler = ExitCodeResultHandler.class)
public class CLI extends CliCommand {

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
         try (Reader r = Files.newBufferedReader(Paths.get(properties))) {
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
            HostnameVerifier verifier = hostnameVerifier != null ? new RegexHostnameVerifier(hostnameVerifier) : null;
            SSLContextSettings sslContext = SSLContextSettings.getInstance("TLS", null, trustManagerFactory.getTrustManagers(), null, verifier);
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

      CliRuntimeRunner cliRunner = new CliRuntimeRunner("batch", runtimeBuilder.build());
      int exitCode = cliRunner
            .args(new String[]{"run", inputFile})
            .execute();
      context.disconnect();
      return CommandResult.valueOf(exitCode);
   }

   private CommandResult interactive(Shell shell) {
      // We now start an interactive CLI
      CommandRegistry commandRegistry = initializeCommands();
      context.setRegistry(commandRegistry);
      CliAliasManager aliasManager;
      try {
         aliasManager = new CliAliasManager(context.getConfigPath().resolve("aliases").toFile(), true, commandRegistry);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      SettingsBuilder settings = SettingsBuilder.builder();
      settings
            .enableAlias(true)
            .aliasManager(aliasManager)
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
         settings.connection(((AeshDelegatingShell) shell).getConnection());
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

   private static AeshCommandRuntimeBuilder initialCommandRuntimeBuilder(Shell shell, Properties properties, boolean kube) throws CommandRegistryException {
      AeshCommandRegistryBuilder registryBuilder = AeshCommandRegistryBuilder.builder();
      Context context;
      if (kube) {
         context = new KubernetesContext(properties);
         registryBuilder.command(Kube.class);
      } else {
         context = new ContextImpl(properties);
         registryBuilder.command(CLI.class);
      }
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

   private static boolean isKubernetesMode() {
      return ProcessInfo.getInstance().getName().contains("kubectl") || Boolean.getBoolean("infinispan.cli.kubernetes");
   }

   public static int main(Shell shell, String[] args, Properties properties, boolean kube) {
      CommandRuntime runtime = null;
      try {
         SecurityActions.addSecurityProvider(WildFlyElytronCredentialStoreProvider.getInstance());
         runtime = initialCommandRuntimeBuilder(shell, properties, kube).build();
         int exitCode = new CliRuntimeRunner(kube ? "kube" : "cli", runtime).args(args).execute();
         return exitCode;
      } catch (Exception e) {
         throw new RuntimeException(e);
      } finally {
         if (runtime != null) {
            Util.close((AutoCloseable) runtime.getAeshContext());
         }
      }
   }

   public static int main(Shell shell, String[] args, Properties properties) {
      return main(shell, args, properties, isKubernetesMode());
   }

   public static void main(String[] args) {
      System.exit(main(new DefaultShell(), args, System.getProperties()));
   }

   public static Path getServerHome(Resource server) {
      if (server != null) {
         return Paths.get(server.getAbsolutePath());
      } else {
         String serverHome = System.getProperty("infinispan.server.home.path");
         if (serverHome != null) {
            return Paths.get(serverHome);
         } else {
            // Fall back to the cwd
            return Paths.get(System.getProperty("user.dir"));
         }
      }
   }
}
