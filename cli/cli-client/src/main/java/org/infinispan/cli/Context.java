package org.infinispan.cli;

import java.util.List;

import javax.net.ssl.SSLContext;

import org.aesh.command.CommandResult;
import org.aesh.command.shell.Shell;
import org.aesh.readline.AeshContext;
import org.aesh.readline.ReadlineConsole;
import org.infinispan.cli.commands.CommandInputLine;
import org.infinispan.cli.connection.Connection;

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

   SSLContext getSslContext();

   void setSslContext(SSLContext sslContext);

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
}
