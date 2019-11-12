package org.infinispan.cli;

import static org.infinispan.cli.logging.Messages.MSG;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.aesh.command.AeshCommandRuntimeBuilder;
import org.aesh.command.Command;
import org.aesh.command.impl.registry.AeshCommandRegistryBuilder;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.registry.CommandRegistryException;
import org.aesh.command.settings.SettingsBuilder;
import org.aesh.command.shell.Shell;
import org.aesh.readline.ReadlineConsole;
import org.aesh.terminal.Connection;
import org.infinispan.cli.activators.ContextAwareCommandActivatorProvider;
import org.infinispan.cli.commands.Batch;
import org.infinispan.cli.completers.ContextAwareCompleterInvocationProvider;
import org.infinispan.cli.impl.CliCommandNotFoundHandler;
import org.infinispan.cli.impl.CliMode;
import org.infinispan.cli.impl.CliRuntimeRunner;
import org.infinispan.cli.impl.ContextAwareCommandInvocationProvider;
import org.infinispan.cli.impl.ContextAwareQuitHandler;
import org.infinispan.cli.impl.ContextImpl;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.util.ZeroSecurityHostnameVerifier;
import org.infinispan.cli.util.ZeroSecurityTrustManager;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Version;
import org.wildfly.security.keystore.KeyStoreUtil;
import org.wildfly.security.provider.util.ProviderUtil;

/**
 * CLI
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class CLI {
   private final PrintStream stdOut;
   private final PrintStream stdErr;
   private ReadlineConsole console;
   private Context context;
   private CliMode mode = CliMode.INTERACTIVE;
   private String inputFile;
   private Connection terminalConnection;
   private Shell shell;

   public CLI() {
      this(System.out, System.err, System.getProperties());
   }

   public CLI(PrintStream stdOut, PrintStream stdErr, Properties properties) {
      this.stdOut = stdOut;
      this.stdErr = stdErr;
      this.context = new ContextImpl(properties);
   }

   public final void run(String[] args) {
      String connectionString = null;
      String trustStorePath = null;
      String trustStorePassword = null;
      boolean trustAll = false;
      Iterator<String> iterator = Arrays.stream(args).iterator();
      while (iterator.hasNext()) {
         String command = iterator.next();
         String parameter = null;

         if (command.startsWith("--")) {
            int equals = command.indexOf('=');
            if (equals > 0) {
               parameter = command.substring(equals + 1);
               command = command.substring(0, equals);
            }
         } else if (command.startsWith("-D")) {
            if (command.length() < 3) {
               stdErr.println(MSG.invalidArgument(command));
               exit(1);
               return;
            } else {
               parameter = command.substring(2);
               command = command.substring(0, 2);
            }
         } else if (command.startsWith("-")) {
            if (command.length() != 2) {
               stdErr.println(MSG.invalidShortArgument(command));
               exit(1);
               return;
            }
         } else {
            stdErr.println(MSG.invalidArgument(command));
            exit(1);
            return;
         }
         switch (command) {
            case "-c":
               parameter = iterator.next();
               // Fall through
            case "--connect":
               connectionString = parameter;
               break;
            case "-f":
               parameter = iterator.next();
               // Fall through
            case "--file":
               inputFile = parameter;
               if ("-".equals(inputFile) || new File(inputFile).isFile()) {
                  mode = CliMode.BATCH;
               } else {
                  stdErr.println(MSG.fileNotExists(inputFile));
                  exit(1);
               }
               break;
            case "-t":
               parameter = iterator.next();
            case "--truststore":
               trustStorePath = parameter;
               break;
            case "-s":
               parameter = iterator.next();
            case "--truststore-password":
               trustStorePassword = parameter;
               break;
            case "--trustall":
               trustAll = true;
               break;
            case "-h":
            case "--help":
               version(stdOut);
               help(stdOut);
               exit(0);
               return;
            case "-D":
               int equals = parameter.indexOf('=');
               context.setProperty(parameter.substring(0, equals), parameter.substring(equals + 1));
               break;
            case "-v":
            case "--version":
               version(stdOut);
               exit(0);
               return;
            default:
               stdErr.println(MSG.unknownArgument(command));
               exit(1);
               return;
         }
      }
      if (trustStorePath != null) {
         try (FileInputStream f = new FileInputStream(trustStorePath)) {
            KeyStore keyStore = KeyStoreUtil.loadKeyStore(ProviderUtil.INSTALLED_PROVIDERS, null, f, trustStorePath, trustStorePassword.toCharArray());
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);
            SSLContextSettings sslContext = SSLContextSettings.getInstance("TLS", null, trustManagerFactory.getTrustManagers(), null, null);
            context.setSslContext(sslContext);
         } catch (Exception e) {
            stdErr.println(MSG.keyStoreError(trustStorePath, e));
            exit(1);
            return;
         }
      } else if (trustAll) {
         SSLContextSettings sslContext = SSLContextSettings.getInstance("TLS", null, new TrustManager[]{new ZeroSecurityTrustManager()}, null, new ZeroSecurityHostnameVerifier());
         context.setSslContext(sslContext);
      }
      if (connectionString != null) {
         context.connect(null, connectionString);
      }
      switch (mode) {
         case BATCH:
            batchRun();
            break;
         case INTERACTIVE:
            interactiveRun();
            break;
      }
   }

   private void batchRun() {
      AeshCommandRegistryBuilder registryBuilder = AeshCommandRegistryBuilder.builder();
      try {
         registryBuilder.command(Batch.class);
         List<Class<? extends Command>> commands = new ArrayList<>();
         for (Command command : ServiceFinder.load(Command.class, this.getClass().getClassLoader())) {
            commands.add(command.getClass());
         }
         registryBuilder.commands(commands);
      } catch (CommandRegistryException e) {
         throw new RuntimeException(e);
      }

      AeshCommandRuntimeBuilder runtimeBuilder = AeshCommandRuntimeBuilder.builder();
      runtimeBuilder
            .commandActivatorProvider(new ContextAwareCommandActivatorProvider(context))
            .commandInvocationProvider(new ContextAwareCommandInvocationProvider(context))
            .commandNotFoundHandler(new CliCommandNotFoundHandler())
            .completerInvocationProvider(new ContextAwareCompleterInvocationProvider(context))
            .aeshContext(context)
            .commandRegistry(registryBuilder.create());
      if (shell != null) {
         runtimeBuilder.shell(shell);
      }

      CliRuntimeRunner cliRunner = CliRuntimeRunner.builder("batch", runtimeBuilder.build());
      cliRunner
            .args(new String[]{"run", inputFile})
            .execute();
   }

   private void interactiveRun() {
      CommandRegistry commandRegistry = initializeCommands();
      context.setRegistry(commandRegistry);
      SettingsBuilder settings = SettingsBuilder.builder();
      settings
            .enableAlias(true)
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
      if (terminalConnection != null) {
         settings.connection(terminalConnection);
      }

      console = new ReadlineConsole(settings.build());
      context.setConsole(console);
      try {
         console.start();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   private CommandRegistry initializeCommands() {
      AeshCommandRegistryBuilder<CommandInvocation> registryBuilder = AeshCommandRegistryBuilder.builder();
      Collection<? extends Command> commands = ServiceFinder.load(Command.class, this.getClass().getClassLoader());
      try {
         for (Command command : commands) {
            registryBuilder.command(command);
         }
      } catch (CommandRegistryException e) {
         throw new RuntimeException(e);
      }
      return registryBuilder.create();
   }

   public Context getContext() {
      return context;
   }

   public void exit(int exitCode) {
      System.exit(exitCode);
   }

   private void help(PrintStream out) {
      out.printf("Usage: cli [OPTION]...\n");
      out.printf("  -c, --connect=URL         %s\n", MSG.cliHelpConnect(Version.getBrandName()));
      out.printf("                            %s\n", MSG.cliHelpConnectHTTP());
      out.printf("  -f, --file=FILE           %s\n", MSG.cliHelpFile());
      out.printf("  -h, --help                %s\n", MSG.cliHelpHelp());
      out.printf("  --trustall                %s\n", MSG.cliHelpTrustAll());
      out.printf("  -s, --truststore-password %s\n", MSG.cliHelpTruststorePassword());
      out.printf("  -t, --truststore          %s\n", MSG.cliHelpTruststore());
      out.printf("  -h, --help                %s\n", MSG.cliHelpHelp());
      out.printf("  -v, --version             %s\n", MSG.cliHelpVersion());
   }

   private void version(PrintStream out) {
      out.printf("%s CLI %s\n", Version.getBrandName(), Version.getVersion());
      out.printf("Copyright (C) Red Hat Inc. and/or its affiliates and other contributors\n");
      out.printf("License Apache License, v. 2.0. http://www.apache.org/licenses/LICENSE-2.0\n");
   }

   public static void main(String[] args) {
      CLI cli = new CLI();
      cli.run(args);
   }

   /**
    * For using a custom terminal connection in tests
    */
   public void setTerminalConnection(Connection terminalConnection) {
      this.terminalConnection = terminalConnection;
   }

   /**
    * For using a custom shell in tests
    */
   public void setShell(Shell shell) {
      this.shell = shell;
   }

   public void stop() {
      context.disconnect();
      if (console != null) {
         console.stop();
      }
   }
}
