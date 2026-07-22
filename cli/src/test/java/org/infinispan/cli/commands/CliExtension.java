package org.infinispan.cli.commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.Executor;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.invocation.CommandInvocationConfiguration;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.shell.Shell;
import org.aesh.console.ReadlineConsole;
import org.aesh.readline.prompt.Prompt;
import org.aesh.terminal.KeyAction;
import org.infinispan.cli.AeshTestShell;
import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.resources.Resource;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that manages CLI lifecycle for offline command tests.
 * Creates a temporary config directory, an {@link AeshTestShell}, and a
 * {@link StubContext} before each test, cleaning up afterward.
 *
 * @since 16.3
 */
public class CliExtension implements BeforeEachCallback, AfterEachCallback {

   private final String readLineResponse;
   private Path configPath;
   private AeshTestShell shell;
   private StubContext context;

   public CliExtension() {
      this(null);
   }

   public CliExtension(String readLineResponse) {
      this.readLineResponse = readLineResponse;
   }

   @Override
   public void beforeEach(ExtensionContext extensionContext) throws IOException {
      configPath = Files.createTempDirectory("cli-test");
      shell = new AeshTestShell(readLineResponse);
      context = new StubContext(configPath);
   }

   @Override
   public void afterEach(ExtensionContext extensionContext) {
      Util.recursiveFileRemove(configPath.toFile());
   }

   public ContextAwareCommandInvocation invocation() {
      return new ContextAwareCommandInvocation(new StubCommandInvocation(shell), context);
   }

   public ContextAwareCommandInvocation invocation(Connection connection, Properties properties) {
      StubContext ctx = new StubContext(configPath, connection, properties);
      return new ContextAwareCommandInvocation(new StubCommandInvocation(shell), ctx);
   }

   public AeshTestShell shell() {
      return shell;
   }

   public Path configPath() {
      return configPath;
   }

   public Properties cliProperties() {
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", configPath.toString());
      return properties;
   }

   public boolean isProcess() {
      return System.getProperty("infinispan.cli.bin") != null;
   }

   public int run(String... args) {
      shell.clear();
      String cliPath = System.getProperty("infinispan.cli.bin");
      if (cliPath == null) {
         try {
            return CLI.main(shell, cliProperties(), args);
         } catch (Exception e) {
            shell.writeln(e.getMessage(), false);
            return 1;
         }
      }
      List<String> cmd = new ArrayList<>();
      cmd.add(cliPath);
      cmd.addAll(Arrays.asList(args));
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.environment().put("ISPN_CLI_DIR", configPath.toString());
      pb.redirectErrorStream(true);
      try {
         Process p = pb.start();
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
               shell.writeln(line, false);
            }
         }
         if (!p.waitFor(30, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new RuntimeException("CLI process timed out after 30 seconds");
         }
         return p.exitValue();
      } catch (IOException | InterruptedException e) {
         throw new RuntimeException("Failed to execute CLI process", e);
      }
   }

   static class StubContext implements Context {
      private final Path configPath;
      private final Connection connection;
      private final Properties properties;

      StubContext(Path configPath) {
         this(configPath, null, new Properties());
      }

      StubContext(Path configPath, Connection connection, Properties properties) {
         this.configPath = configPath;
         this.connection = connection;
         this.properties = properties;
      }

      @Override
      public Path configPath() {
         return configPath;
      }

      @Override
      public boolean isConnected() {
         return connection != null;
      }

      @Override
      public Connection connection() {
         return connection;
      }

      @Override
      public Properties properties() {
         return properties;
      }

      @Override
      public String getProperty(String key) {
         return properties.getProperty(key);
      }

      @Override
      public String getProperty(Property property, String defaultValue) {
         return properties.getProperty(property.propertyName(), defaultValue);
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
      public void resetProperties() {
         properties.clear();
      }

      @Override
      public void saveProperties() {
      }

      @Override
      public void connect(Shell shell, String connectionString) {
      }

      @Override
      public void connect(Shell shell, String connectionString, String username, String password) {
      }

      @Override
      public void disconnect() {
      }

      @Override
      public void setSslContext(SSLContextSettings sslContext) {
      }

      @Override
      public void setRegistry(CommandRegistry<? extends CommandInvocation> registry) {
      }

      @Override
      public void setConsole(ReadlineConsole console) {
      }

      @Override
      public CommandRegistry<? extends CommandInvocation> getRegistry() {
         return null;
      }

      @Override
      public MediaType getEncoding() {
         return null;
      }

      @Override
      public void setEncoding(MediaType encoding) {
      }

      @Override
      public void refreshPrompt() {
      }

      @Override
      public CommandResult changeResource(Class<? extends Resource> fromResource, String resourceType, String name) throws CommandException {
         return null;
      }

      @Override
      public org.aesh.io.Resource getCurrentWorkingDirectory() {
         return null;
      }

      @Override
      public void setCurrentWorkingDirectory(org.aesh.io.Resource resource) {
      }

      @Override
      public Set<String> exportedVariableNames() {
         return Set.of();
      }

      @Override
      public String exportedVariable(String name) {
         return null;
      }
   }

   static class StubConnection implements Connection {
      private final String uri;
      private final String username;

      StubConnection(String uri, String username) {
         this.uri = uri;
         this.username = username;
      }

      @Override
      public void connect() {
      }

      @Override
      public void connect(String username, String password) {
      }

      @Override
      public String getURI() {
         return uri;
      }

      @Override
      public String getUsername() {
         return username;
      }

      @Override
      public String execute(BiFunction<RestClient, Resource, CompletionStage<RestResponse>> op, ResponseMode responseMode) {
         return null;
      }

      @Override
      public Resource getActiveResource() {
         return null;
      }

      @Override
      public void setActiveResource(Resource resource) {
      }

      @Override
      public Resource getActiveContainer() {
         return null;
      }

      @Override
      public Collection<String> getAvailableCaches() {
         return null;
      }

      @Override
      public Collection<String> getAvailableContainers() {
         return null;
      }

      @Override
      public Collection<String> getAvailableCounters() {
         return null;
      }

      @Override
      public Collection<String> getAvailableCacheConfigurations() {
         return null;
      }

      @Override
      public Collection<String> getAvailableSchemas() {
         return null;
      }

      @Override
      public Collection<String> getAvailableServers() {
         return null;
      }

      @Override
      public Collection<String> getAvailableSites(String cache) {
         return null;
      }

      @Override
      public Collection<String> getAvailableTasks() {
         return null;
      }

      @Override
      public Iterable<Map<String, String>> getCacheKeys(String cache) {
         return null;
      }

      @Override
      public Iterable<Map<String, String>> getCacheKeys(String cache, int limit) {
         return null;
      }

      @Override
      public Iterable<Map<String, String>> getCacheEntries(String cache, int limit, boolean metadata) {
         return null;
      }

      @Override
      public Iterable<String> getCounterValue(String counter) {
         return null;
      }

      @Override
      public boolean isConnected() {
         return true;
      }

      @Override
      public String describeContainer() {
         return null;
      }

      @Override
      public String describeCache(String cache) {
         return null;
      }

      @Override
      public String describeKey(String cache, String key) {
         return null;
      }

      @Override
      public String describeConfiguration(String configuration) {
         return null;
      }

      @Override
      public String describeCounter(String counter) {
         return null;
      }

      @Override
      public String describeTask(String taskName) {
         return null;
      }

      @Override
      public String getConnectionInfo() {
         return null;
      }

      @Override
      public String getServerVersion() {
         return null;
      }

      @Override
      public Collection<String> getClusterNodes() {
         return null;
      }

      @Override
      public Collection<String> getAvailableLogAppenders() {
         return null;
      }

      @Override
      public Collection<String> getAvailableLoggers() {
         return null;
      }

      @Override
      public Collection<String> getBackupNames() {
         return null;
      }

      @Override
      public Collection<String> getSitesView() {
         return null;
      }

      @Override
      public String getLocalSiteName() {
         return null;
      }

      @Override
      public boolean isRelayNode() {
         return false;
      }

      @Override
      public Collection<String> getRelayNodes() {
         return null;
      }

      @Override
      public Collection<String> getConnectorNames() {
         return null;
      }

      @Override
      public MediaType getEncoding() {
         return null;
      }

      @Override
      public void setEncoding(MediaType encoding) {
      }

      @Override
      public void refreshServerInfo() {
      }

      @Override
      public Collection<String> getDataSourceNames() {
         return null;
      }

      @Override
      public Collection<String> getCacheConfigurationAttributes(String name) {
         return null;
      }

      @Override
      public Collection<String> getRoles() {
         return null;
      }

      @Override
      public void close() {
      }
   }

   record StubCommandInvocation(Shell shell) implements CommandInvocation {
      @Override
      public Shell getShell() {
         return shell;
      }

      @Override
      public void setPrompt(Prompt prompt) {
      }

      @Override
      public Prompt getPrompt() {
         return null;
      }

      @Override
      public String getHelpInfo(String commandName) {
         return "";
      }

      @Override
      public String getHelpInfo() {
         return "";
      }

      @Override
      public void stop() {
      }

      @Override
      public CommandInvocationConfiguration getConfiguration() {
         return null;
      }

      @Override
      public KeyAction input() {
         return null;
      }

      @Override
      public KeyAction input(long timeout, TimeUnit unit) {
         return null;
      }

      @Override
      public String inputLine() {
         return null;
      }

      @Override
      public String inputLine(Prompt prompt) {
         return null;
      }

      @Override
      public void executeCommand(String input) {
      }

      @Override
      public Executor<? extends CommandInvocation> buildExecutor(String line) {
         return null;
      }

      @Override
      public void print(String msg) {
         shell.write(msg, false);
      }

      @Override
      public void println(String msg) {
         shell.writeln(msg, false);
      }

      @Override
      public void print(String msg, boolean paging) {
         shell.write(msg, paging);
      }

      @Override
      public void println(String msg, boolean paging) {
         shell.writeln(msg, paging);
      }
   }
}
