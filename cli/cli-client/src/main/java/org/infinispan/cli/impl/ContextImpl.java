package org.infinispan.cli.impl;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.shell.Shell;
import org.aesh.io.FileResource;
import org.aesh.readline.AeshContext;
import org.aesh.readline.Prompt;
import org.aesh.readline.ReadlineConsole;
import org.aesh.readline.terminal.formatting.Color;
import org.aesh.readline.terminal.formatting.TerminalColor;
import org.aesh.readline.terminal.formatting.TerminalString;
import org.aesh.terminal.utils.ANSI;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.CommandInputLine;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.ConnectionFactory;
import org.infinispan.cli.logging.Messages;
import org.infinispan.cli.resources.Resource;
import org.infinispan.cli.util.SystemUtils;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;

/**
 * ContextImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ContextImpl implements Context, AeshContext {
   private static final String CONFIG_FILE = "cli.properties";
   private Connection connection;
   private final Properties properties;
   private org.aesh.io.Resource cwd;
   private ReadlineConsole console;
   private SSLContextSettings sslContext;
   private CommandRegistry<? extends CommandInvocation> registry;
   private final Path configPath;

   public ContextImpl(Properties defaults) {
      this.properties = new Properties(defaults);
      String userDir = properties.getProperty("user.dir");
      cwd = userDir != null ? new FileResource(userDir) : null;
      String cliDir = properties.getProperty("cli.dir");
      if (cliDir == null) {
         cliDir = System.getenv("ISPN_CLI_DIR");
      }
      if (cliDir != null) {
         configPath = Paths.get(cliDir);
      } else {
         configPath = Paths.get(SystemUtils.getAppConfigFolder(Version.getBrandName().toLowerCase().replace(' ', '_')));
      }
      Path configFile = configPath.resolve(CONFIG_FILE);
      if (Files.exists(configFile)) {
         try (Reader r = Files.newBufferedReader(configFile)) {
            properties.load(r);
         } catch (IOException e) {
            System.err.println(Messages.MSG.configLoadFailed(configFile.toString()));
         }
      }
   }

   @Override
   public Path getConfigPath() {
      return configPath;
   }

   @Override
   public boolean isConnected() {
      return connection != null && connection.isConnected();
   }

   @Override
   public void setProperty(String key, String value) {
      if (value == null) {
         properties.remove(key);
      } else {
         properties.setProperty(key, value);
      }
   }

   @Override
   public String getProperty(String key) {
      return properties.getProperty(key);
   }

   @Override
   public String getProperty(Property property) {
      return properties.getProperty(property.propertyName());
   }

   @Override
   public Properties getProperties() {
      return properties;
   }

   @Override
   public void saveProperties() {
      Path configFile = configPath.resolve(CONFIG_FILE);
      try {
         Files.createDirectories(configPath);
         try (Writer w = Files.newBufferedWriter(configFile)) {
            properties.store(w, null);
         }
      } catch (IOException e) {
         System.err.println(Messages.MSG.configStoreFailed(configFile.toString()));
      }
   }

   @Override
   public void setSslContext(SSLContextSettings sslContext) {
      this.sslContext = sslContext;
   }

   @Override
   public Connection connect(Shell shell, String connectionString) {
      disconnect();
      connection = ConnectionFactory.getConnection(connectionString, sslContext);
      // Attempt a connection. If we receive an exception we might need credentials
      try {
         connection.connect();
      } catch (AccessDeniedException accessDenied) {
         try {
            Util.close(connection);
            String username = null;
            String password = null;
            if (shell != null) {
               username = shell.readLine(Messages.MSG.username());
               password = username.isEmpty() ? "" : shell.readLine(new Prompt(Messages.MSG.password(), '*'));
            } else {
               java.io.Console sysConsole = System.console();
               if (sysConsole != null) {
                  username = sysConsole.readLine(Messages.MSG.username());
                  password = username.isEmpty() ? "" : new String(sysConsole.readPassword(Messages.MSG.password()));
               } else {
               }
            }
            connection.connect(username, password);
         } catch (Exception e) {
            disconnect();
            showError(shell, e);
         }
      } catch (IOException e) {
         disconnect();
         showError(shell, e);
      }
      refreshPrompt();
      return connection;
   }

   private void showError(Shell shell, Throwable t) {
      if (shell != null) {
         shell.writeln(t.getMessage());
      } else {
         System.err.println(t.getMessage());
      }
   }

   @Override
   public Connection connect(Shell shell, String connectionString, String username, String password) {
      disconnect();
      connection = ConnectionFactory.getConnection(connectionString, sslContext);
      try {
         connection.connect(username, password);
      } catch (IOException e) {
         if (shell != null) {
            shell.writeln(ANSI.RED_TEXT + e.getLocalizedMessage() + ANSI.DEFAULT_TEXT);
         } else {
            System.err.println(e.getLocalizedMessage());
         }
      }
      refreshPrompt();
      return connection;
   }

   private void buildPrompt(Resource resource, StringBuilder builder) {
      if (resource != null) {
         if (resource.getParent() != null) {
            buildPrompt(resource.getParent(), builder);
         }
         builder.append("/").append(resource.getName());
      }
   }

   private void refreshPrompt() {
      if (console != null) {
         if (connection != null) {
            StringBuilder prompt = new StringBuilder();
            prompt.append("[").append(ANSI.GREEN_TEXT).append(connection.getConnectionInfo()).append(ANSI.DEFAULT_TEXT);
            buildPrompt(connection.getActiveResource(), prompt);
            prompt.append("]> ");
            console.setPrompt(prompt.toString());
         } else {
            console.setPrompt("[" + ANSI.YELLOW_TEXT + "disconnected" + ANSI.DEFAULT_TEXT + "]> ");
         }
      }
   }

   @Override
   public void disconnect() {
      Util.close(connection);
      connection = null;
      refreshPrompt();
   }

   @Override
   public CommandResult execute(Shell shell, List<CommandInputLine> commands) {
      try {
         String response = connection.execute(commands);
         if (response != null && !response.isEmpty()) {
            shell.writeln(response);
         }
         refreshPrompt();
         return CommandResult.SUCCESS;
      } catch (Exception e) {
         TerminalString error = new TerminalString(Util.getRootCause(e).getLocalizedMessage(), new TerminalColor(Color.RED, Color.DEFAULT, Color.Intensity.BRIGHT));
         shell.writeln(error.toString());
         refreshPrompt();
         return CommandResult.FAILURE;
      }
   }

   @Override
   public void setConsole(ReadlineConsole console) {
      this.console = console;
      refreshPrompt();
   }

   @Override
   public CommandRegistry<? extends CommandInvocation> getRegistry() {
      return registry;
   }

   @Override
   public void setRegistry(CommandRegistry<? extends CommandInvocation> registry) {
      this.registry = registry;
   }

   @Override
   public Connection getConnection() {
      return connection;
   }

   @Override
   public org.aesh.io.Resource getCurrentWorkingDirectory() {
      return cwd;
   }

   @Override
   public void setCurrentWorkingDirectory(org.aesh.io.Resource cwd) {
      this.cwd = cwd;
   }

   @Override
   public Set<String> exportedVariableNames() {
      return Collections.emptySet();
   }

   @Override
   public String exportedVariable(String key) {
      return null;
   }
}
