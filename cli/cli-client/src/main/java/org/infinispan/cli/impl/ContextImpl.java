package org.infinispan.cli.impl;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
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
import org.infinispan.commons.util.Version;

/**
 * ContextImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ContextImpl implements Context, AeshContext {
   private final ConfigImpl config;
   private Connection connection;
   private final Properties properties;
   private org.aesh.io.Resource cwd;
   private ReadlineConsole console;
   private SSLContextSettings sslContext;
   private CommandRegistry<? extends CommandInvocation> registry;

   public ContextImpl(Properties properties) {
      this.properties = properties;
      String userDir = properties.getProperty("user.dir");
      cwd = userDir != null ? new FileResource(userDir) : null;
      config = new ConfigImpl(SystemUtils.getAppConfigFolder(Version.getBrandName().toLowerCase().replace(' ', '_')));
      config.load();
   }

   @Override
   public boolean isConnected() {
      return connection != null && connection.isConnected();
   }

   @Override
   public void setProperty(String key, String value) {
      properties.setProperty(key, value);
   }

   @Override
   public String getProperty(String key) {
      return properties.getProperty(key);
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
            String username = null;
            String password = null;
            if (shell != null) {
               username = shell.readLine(Messages.MSG.username());
               password = username.isEmpty() ? null : shell.readLine(new Prompt(Messages.MSG.password(), '*'));
            } else {
               java.io.Console sysConsole = System.console();
               if (sysConsole != null) {
                  username = sysConsole.readLine(Messages.MSG.username());
                  password = username.isEmpty() ? null : new String(sysConsole.readPassword(Messages.MSG.password()));
               } else {
               }
            }
            connection.connect(username, password);
         } catch (Exception e) {
            connection = null;
            showError(shell, e);
         }
      } catch (IOException e) {
         connection = null;
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
            shell.writeln(ANSI.RED_TEXT + e.getMessage() + ANSI.DEFAULT_TEXT);
         } else {
            System.err.println(e.getMessage());
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
      if (connection != null) {
         try {
            connection.close();
         } catch (IOException e) {
         }
         connection = null;
      }
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
         TerminalString error = new TerminalString(e.getMessage(), new TerminalColor(Color.RED, Color.DEFAULT, Color.Intensity.BRIGHT));
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
