package org.infinispan.cli.shell;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.fusesource.jansi.Ansi;
import org.infinispan.cli.Config;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.Command;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.commands.server.Ping;
import org.infinispan.cli.commands.server.Version;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.ConnectionFactory;
import org.infinispan.cli.impl.CommandBufferImpl;
import org.infinispan.cli.impl.ContextImpl;
import org.infinispan.cli.io.ConsoleIOAdapter;
import org.infinispan.cli.io.StreamIOAdapter;
import org.infinispan.cli.util.SystemUtils;
import org.jboss.aesh.console.Console;
import org.jboss.aesh.console.settings.Settings;

/**
 *
 * ShellImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ShellImpl implements Shell {
   private static final int SESSION_PING_TIMEOUT = 30;
   private Config config;
   private Console console;
   private Context context;
   private ShellMode mode = ShellMode.INTERACTIVE;
   private String inputFile;
   private ScheduledFuture<?> sessionPingTask;

   @Override
   public void init(final String[] args) throws Exception {
      // Initialize the context for simple standard I/O
      context = new ContextImpl(new StreamIOAdapter(), new CommandBufferImpl());
      String sopts = "c:f:hv";
      LongOpt[] lopts = new LongOpt[] { new LongOpt("connect", LongOpt.REQUIRED_ARGUMENT, null, 'c'),
            new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f'),
            new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'),
            new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'v'), };
      Getopt g = new Getopt("ispn-cli", args, sopts, lopts);
      int c;
      while ((c = g.getopt()) != -1) {
         switch (c) {
         case 'c':
            Connection connection = ConnectionFactory.getConnection(g.getOptarg());
            String password = null;
            if (connection.needsCredentials()) {
               java.io.Console sysConsole = System.console();
               if (sysConsole != null) {
                  password = new String(sysConsole.readPassword("Password: "));
               } else {
                  exitWithError("Cannot read password non-interactively");
               }
            }
            connection.connect(password);
            context.setConnection(connection);
            break;
         case 'f':
            inputFile = g.getOptarg();
            if ("-".equals(inputFile) || new File(inputFile).isFile()) {
               mode = ShellMode.BATCH;
            } else {
               exitWithError("File '%s' doesn't exist or is not a file", g.getOptarg());
            }
            break;
         case 'h':
            help();
            break;
         case 'v':
            version();
            break;
         }
      }
   }

   private void exitWithError(final String format, final Object... args) {
      System.err.printf(format, args);
      System.err.println();
      System.exit(1);
   }

   @Override
   public void run() throws IOException {
      switch (mode) {
      case BATCH:
         batchRun();
         break;
      case INTERACTIVE:
         interactiveRun();
         break;
      }
   }

   private void batchRun() throws IOException {
      Reader r = "-".equals(inputFile) ? new InputStreamReader(System.in) : new FileReader(inputFile);
      BufferedReader br = new BufferedReader(r);
      try {
         for (String line = br.readLine(); line != null; line = br.readLine()) {
            execute(line);
         }
      } finally {
         br.close();
      }
   }

   private void interactiveRun() throws IOException {
      config = new ConfigImpl(SystemUtils.getAppConfigFolder("InfinispanShell"));
      config.load();
      Settings settings = Settings.getInstance();
      settings.setAliasEnabled(false);
      console = new Console();
      context.setOutputAdapter(new ConsoleIOAdapter(console));
      console.addCompletion(new Completer(context));
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
      sessionPingTask = executor.scheduleWithFixedDelay(new PingTask(), SESSION_PING_TIMEOUT, SESSION_PING_TIMEOUT, TimeUnit.SECONDS);

      while (!context.isQuitting()) {
         try {
            context.refreshProperties();
            String line = console.read(getPrompt()).getBuffer();

            if (line != null) {
               if (!"".equals(line.trim())) {
                  execute(line);
               }
            }
         } catch (Exception e) {
            context.error(e);
         }
      }
      try {
         sessionPingTask.cancel(true);
         executor.shutdownNow();
         config.save();
         console.stop();
      } catch (Exception e) {
      }
   }

   private void execute(final String line) {
      ProcessedCommand parsed = new ProcessedCommand(line);
      Command command = context.getCommandRegistry().getCommand(parsed.getCommand());
      if (command != null) {
         command.execute(context, parsed);
      } else {
         context.error("Command " + parsed.getCommand() + " unknown or not available");
      }
   }

   private String getPrompt() {
      return Prompt.echo(this, Prompt.promptExpressionParser(this, config.getPrompt()));
   }

   @Override
   public String renderColor(final Color color, final String output) {
      if (!config.isColorEnabled()) {
         return output;
      }

      Ansi ansi = new Ansi();

      switch (color) {
      case BLACK:
         ansi.fg(Ansi.Color.BLACK);
         break;
      case BLUE:
         ansi.fg(Ansi.Color.BLUE);
         break;
      case CYAN:
         ansi.fg(Ansi.Color.CYAN);
         break;
      case GREEN:
         ansi.fg(Ansi.Color.GREEN);
         break;
      case MAGENTA:
         ansi.fg(Ansi.Color.MAGENTA);
         break;
      case RED:
         ansi.fg(Ansi.Color.RED);
         break;
      case WHITE:
         ansi.fg(Ansi.Color.WHITE);
         break;
      case YELLOW:
         ansi.fg(Ansi.Color.YELLOW);
         break;
      case BOLD:
         ansi.a(Ansi.Attribute.INTENSITY_BOLD);
         break;
      case ITALIC:
         ansi.a(Ansi.Attribute.ITALIC);
         ansi.a(Ansi.Attribute.INTENSITY_FAINT);
         break;

      default:
         return output;
      }

      return ansi.render(output).reset().toString();
   }

   @Override
   public String getCWD() {
      File directory = new File(".");
      return directory.getAbsolutePath();
   }

   @Override
   public Context getContext() {
      return context;
   }

   private void help() {
      System.out.println("Usage: ispn-cli [OPTION]...");
      System.out.println("Command-line interface for interacting with a running instance of Infinispan");
      System.out.println();
      System.out.println("Options:");
      System.out.println("  -c, --connect=URL       connects to a running instance of Infinispan. ");
      System.out.println("                          JMX over RMI jmx://[username[:password]]@host:port[/container[/cache]]");
      System.out.println("                          JMX over JBoss remoting remoting://[username[:password]]@host:port[/container[/cache]]");
      System.out.println("  -f, --file=FILE         reads input from the specified file instead of using ");
      System.out.println("                          interactive mode. If FILE is '-', then commands will be read");
      System.out.println("                          from stdin");
      System.out.println("  -h, --help              shows this help page");
      System.out.println("  -v, --version           shows version information");
      System.exit(0);
   }

   private void version() {
      System.out.println("ispn-cli " + Version.class.getPackage().getImplementationVersion());
      System.out.println("Copyright (C) 2009-2014 Red Hat Inc. and/or its affiliates and other contributors");
      System.out.println("License Apache License, v. 2.0. http://www.apache.org/licenses/LICENSE-2.0");
      System.exit(0);
   }

   class PingTask implements Runnable {
      Ping ping = new Ping();

      @Override
      public void run() {
         if(context.isConnected()) {
            ping.execute(context, null);
         }
      }
   }
}
