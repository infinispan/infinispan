package org.infinispan.cli;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.shell.Shell;
import org.aesh.readline.AeshContext;
import org.aesh.readline.ReadlineConsole;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.SSLContextSettings;
import org.infinispan.cli.resources.Resource;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * Context.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Context extends AeshContext {
   Path getConfigPath();

   boolean isConnected();

   void setProperty(String key, String value);

   String getProperty(String key);

   String getProperty(Property property);

   Properties getProperties();

   void resetProperties();

   void saveProperties();

   void setSslContext(SSLContextSettings sslContext);

   /**
    * Connects to a server
    *
    * @param shell
    * @param connectionString
    * @return
    */
   Connection connect(Shell shell, String connectionString);

   /**
    * Connect to a server using the supplied username and password
    *
    * @param shell
    * @param connectionString
    * @param username
    * @param password
    * @return
    */
   Connection connect(Shell shell, String connectionString, String username, String password);

   void setRegistry(CommandRegistry<? extends CommandInvocation> registry);

   /**
    * Returns the current {@link Connection}
    *
    * @return
    */
   Connection getConnection();

   /**
    * Disconnects from the server
    */
   void disconnect();

   void setConsole(ReadlineConsole console);

   CommandRegistry<? extends CommandInvocation> getRegistry();

   MediaType getEncoding();

   void setEncoding(MediaType encoding);

   void refreshPrompt();

   CommandResult changeResource(Class<? extends Resource> fromResource, String resourceType, String name) throws CommandException;

   enum Property {
      TRUSTALL,
      TRUSTSTORE,
      TRUSTSTORE_PASSWORD,
      KEYSTORE,
      KEYSTORE_PASSWORD,
      PROVIDER,
      AUTOCONNECT_URL,
      AUTOEXEC;

      public static final List<String> NAMES;

      static {
         Property[] values = values();
         final List<String> names = new ArrayList<>(values.length);
         for (Property element : values) {
            names.add(element.propertyName());
         }
         NAMES = names;
      }

      public String propertyName() {
         return name().toLowerCase().replace('_', '-');
      }
   }
}
