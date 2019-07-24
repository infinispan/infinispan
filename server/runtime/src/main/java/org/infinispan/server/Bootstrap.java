package org.infinispan.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;
import java.util.logging.LogManager;

import org.infinispan.Version;
import org.infinispan.server.tool.Main;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Bootstrap extends Main {
   private final ExitHandler exitHandler;
   private File configurationFile;

   public Bootstrap(PrintStream stdOut, PrintStream stdErr, ExitHandler exitHandler, Properties properties) {
      super(stdOut, stdErr, properties);
      this.exitHandler = exitHandler;
   }

   public static void main(String[] args) {
      Bootstrap bootstrap = new Bootstrap(System.out, System.err, new DefaultExitHandler(), System.getProperties());
      bootstrap.run(args);
   }

   @Override
   protected void handleArgumentCommand(String command, String parameter, Iterator<String> args) {
      switch (command) {
         case "-c":
            parameter = args.next();
            // Fall through
         case "--server-config":
            configurationFile = new File(parameter);
            break;
         case "-s":
            parameter = args.next();
            // Fall through
         case "--server-root":
            serverRoot = new File(parameter);
            break;
         case "-b":
            parameter = args.next();
            // Fall through
         case "--bind-address":
            properties.setProperty(Server.INFINISPAN_BIND_ADDRESS, parameter);
            break;
         case "-n":
            parameter = args.next();
         case "--node-name":
            properties.setProperty(Server.INFINISPAN_NODE_NAME, parameter);
            break;
         case "-g":
            parameter = args.next();
         case "--cluster-name":
            properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, parameter);
            break;
         case "-o":
            parameter = args.next();
            // Fall through
         case "--port-offset":
            properties.setProperty(Server.INFINISPAN_PORT_OFFSET, parameter);
            break;
         default:
            throw new IllegalArgumentException();
      }
   }

   public void runInternal() {
      File confDir = new File(serverRoot, Server.DEFAULT_SERVER_CONFIG);
      if (configurationFile == null) {
         configurationFile = new File(confDir, Server.DEFAULT_CONFIGURATION_FILE);
      } else if (!configurationFile.isAbsolute()) {
         configurationFile = Paths.get(confDir.getPath(), configurationFile.getPath()).toFile();
      }
      File logDir = new File(serverRoot, Server.DEFAULT_SERVER_LOG);
      properties.putIfAbsent(Server.INFINISPAN_SERVER_LOG_PATH, logDir.getAbsolutePath());

      try (InputStream is = new FileInputStream(new File(confDir, "logging.properties"))) {
         LogManager.getLogManager().readConfiguration(is);
      } catch (IOException e) {
         stdErr.printf("Could not load logging.properties: %s", e.getMessage());
         e.printStackTrace(stdErr);
      }

      try {
         Runtime.getRuntime().addShutdownHook(new ShutdownHook(exitHandler));
         Server.log.serverStarting(Version.getBrandName());
         Server.log.serverConfiguration(configurationFile.getAbsolutePath());
         Server server = new Server(serverRoot, configurationFile, properties);
         server.setExitHandler(exitHandler);
         server.run().get();
      } catch (Exception e) {
         Server.log.fatal("Error", e);
      }
   }

   @Override
   public void help(PrintStream out) {
      out.printf("Usage:\n");
      out.printf("  -b, --bind-address=<address>  Binds the server to the specified address.\n");
      out.printf("  -c, --server-config=<config>  Uses the specified configuration file. Defaults to '%s'.\n", Server.DEFAULT_CONFIGURATION_FILE);
      out.printf("  -h, --help                    Displays this message and exits.\n");
      out.printf("  -g, --cluster-name=<name>     Sets the name of the cluster.\n");
      out.printf("  -n, --node-name=<name>        Sets the name of this node. Must be unique across the cluster.\n");
      out.printf("  -o, --port-offset=<offset>    Adds the specified offset to all ports.\n");
      out.printf("  -s, --server-root=<path>      Uses the specified path as root for the server. Defaults to '%s'.\n", Server.DEFAULT_SERVER_ROOT_DIR);
      out.printf("  -v, --version                 Displays version information and exits.\n");
      out.printf("  -D<name>=<value>              Sets a system property to the specified value.\n");
   }

   @Override
   public void version(PrintStream out) {
      out.printf("%s Server %s (%s)\n", Version.getBrandName(), Version.getVersion(), Version.getCodename());
   }
}
