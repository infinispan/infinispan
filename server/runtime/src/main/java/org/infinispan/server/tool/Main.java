package org.infinispan.server.tool;

import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.infinispan.server.Server;
import org.infinispan.server.logging.Messages;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public abstract class Main {

   protected final PrintStream stdOut;
   protected final PrintStream stdErr;
   protected final Properties properties;
   protected final String cwd;
   protected File serverHome;
   protected File serverRoot;

   public Main() {
      this(System.out, System.err, System.getProperties());
   }

   public Main(PrintStream stdOut, PrintStream stdErr, Properties properties) {
      this.stdOut = stdOut;
      this.stdErr = stdErr;
      this.properties = properties;
      this.cwd = properties.getProperty("user.dir");
      // Retrieve the server home from the properties if present, otherwise use the current working directory
      serverHome = new File(properties.getProperty(Server.INFINISPAN_SERVER_HOME_PATH, cwd));
      // Retrieve the server root from the properties if present, otherwise use the default server root under the server home
      serverRoot = new File(properties.getProperty(Server.INFINISPAN_SERVER_ROOT_PATH, new File(serverHome, Server.DEFAULT_SERVER_ROOT_DIR).getAbsolutePath()));
   }

   public final void run(String... args) {
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
               stdErr.println(Messages.MSG.invalidArgument(command));
               exit(1);
               return;
            } else {
               parameter = command.substring(2);
               command = command.substring(0, 2);
            }
         } else if (command.startsWith("-")) {
            if (command.length() != 2) {
               stdErr.println(Messages.MSG.invalidShortArgument(command));
               exit(1);
               return;
            }
         } else {
            stdErr.println(Messages.MSG.invalidArgument(command));
            exit(1);
            return;
         }
         switch (command) {
            case "-h":
            case "--help":
               version(stdOut);
               help(stdOut);
               exit(0);
               return;
            case "-D":
               int equals = parameter.indexOf('=');
               properties.setProperty(parameter.substring(0, equals), parameter.substring(equals + 1));
               break;
            case "-v":
            case "--version":
               version(stdOut);
               exit(0);
               return;
            default:
               try {
                  handleArgumentCommand(command, parameter, iterator);
               } catch (IllegalArgumentException e) {
                  stdErr.println(Messages.MSG.unknownArgument(command));
                  exit(1);
                  return;
               }
               break;
         }
      }
      runInternal();
   }

   protected abstract void runInternal();

   protected abstract void handleArgumentCommand(String command, String parameter, Iterator<String> args);

   public void exit(int exitCode) {
      System.exit(exitCode);
   }

   public abstract void help(PrintStream out);

   public abstract void version(PrintStream out);
}
