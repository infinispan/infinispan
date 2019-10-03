package org.infinispan.cli;

import java.util.List;

import org.aesh.command.CommandResult;
import org.aesh.command.invocation.CommandInvocation;
import org.aesh.command.registry.CommandRegistry;
import org.aesh.command.shell.Shell;
import org.aesh.readline.AeshContext;
import org.aesh.readline.ReadlineConsole;
import org.infinispan.cli.commands.CommandInputLine;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.impl.SSLContextSettings;

/**
 * Context.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Context extends AeshContext {
   boolean isConnected();

   void setProperty(String key, String value);

   String getProperty(String key);

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

   CommandResult execute(Shell shell, List<CommandInputLine> commands);

   void setConsole(ReadlineConsole console);

   CommandRegistry<? extends CommandInvocation> getRegistry();
}
