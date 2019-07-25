package org.infinispan.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.logging.LogManager;

import org.infinispan.Version;
import org.infinispan.server.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Bootstrap {

   public static void main(String args[]) {
      main(args, System.out, System.err, new DefaultExitHandler(), SecurityActions.getSystemProperties());
   }

   public static void main(String args[], PrintStream stdOut, PrintStream stdErr, ExitHandler exitHandler, Properties properties) {
      String cwd = properties.getProperty("user.dir");
      // Retrieve the server home from the properties if present, otherwise use the current working directory
      File serverHome = new File(properties.getProperty(Server.INFINISPAN_SERVER_HOME_PATH, cwd));
      // Retrieve the server root from the properties if present, otherwise use the default server root under the server home
      File serverRoot = new File(properties.getProperty(Server.INFINISPAN_SERVER_ROOT_PATH, new File(serverHome, Server.DEFAULT_SERVER_ROOT_DIR).getAbsolutePath()));
      File configurationFile = null;

      for (int i = 0; i < args.length; i++) {
         String command = args[i];
         String parameter = null;
         if (command.startsWith("--")) {
            int equals = command.indexOf('=');
            if (equals > 0) {
               parameter = command.substring(equals + 1);
               command = command.substring(0, equals);
            }
         } else if (command.startsWith("-D")) {
            if (command.length() < 3) {
               stdErr.println(Messages.MSG.invalidArgument(command));
               exitHandler.exit(1);
               return;
            } else {
               parameter = command.substring(2);
               command = command.substring(0, 2);
            }
         } else if (command.startsWith("-")) {
            if (command.length() != 2) {
               stdErr.println(Messages.MSG.invalidShortArgument(command));
               exitHandler.exit(1);
            }
         } else {
            stdErr.println(Messages.MSG.invalidArgument(command));
            exitHandler.exit(1);
         }
         switch (command) {
            case "-h":
            case "--help":
               help(stdOut, true);
               exitHandler.exit(0);
               break;
            case "-c":
               parameter = args[++i];
               // Fall through
            case "--server-config":
               configurationFile = new File(parameter);
               break;
            case "--s":
               parameter = args[++i];
               // Fall through
            case "--server-root":
               serverRoot = new File(parameter);
               break;
            case "-b":
               parameter = args[++i];
               // Fall through
            case "--bind-address":
               properties.setProperty(Server.INFINISPAN_BIND_ADDRESS, parameter);
               break;
            case "-o":
               parameter = args[++i];
               // Fall through
            case "--port-offset":
               properties.setProperty(Server.INFINISPAN_PORT_OFFSET, parameter);
               break;
            case "-D":
               int equals = parameter.indexOf('=');
               properties.setProperty(parameter.substring(0, equals), parameter.substring(equals + 1));
               break;
            case "-v":
            case "--version":
               help(stdOut, false);
               exitHandler.exit(0);
               break;
            default:
               stdErr.println(Messages.MSG.unknownArgument(args[i]));
               exitHandler.exit(1);
               break;
         }
      }

      File confDir = new File(serverRoot, Server.DEFAULT_SERVER_CONFIG);
      if (configurationFile == null)
         configurationFile = new File(confDir, Server.DEFAULT_CONFIGURATION_FILE);
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

   public static void help(PrintStream stdOut, boolean usage) {
      stdOut.printf("%s Server %s (%s)\n", Version.getBrandName(), Version.getVersion(), Version.getCodename());
      if (usage) {
         stdOut.printf("Usage:\n");
         stdOut.printf("  -b, --bind-address=<address>  Binds the server to the specified address.\n");
         stdOut.printf("  -o, --port-offset=<offset>    Adds the specified offset to all ports.\n");
         stdOut.printf("  -c, --server-config=<config>  Uses the specified configuration file. Defaults to '%s'.\n", Server.DEFAULT_CONFIGURATION_FILE);
         stdOut.printf("  -h, --help                    Displays this message and exits.\n");
         stdOut.printf("  -s, --server-root=<path>      Uses the specified path as root for the server. Defaults to '%s'.\n", Server.DEFAULT_SERVER_ROOT_DIR);
         stdOut.printf("  -v, --version                 Displays version information and exits.\n");
         stdOut.printf("  -D<name>=<value>              Sets a system property to the specified value.\n");
      }
   }
}
