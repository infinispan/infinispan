package org.infinispan.cli;

import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.io.IOAdapter;

import java.util.List;

/**
 *
 * Context.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Context {
   boolean isConnected();

   boolean isQuitting();

   void setQuitting(boolean quitting);

   void setProperty(String key, String value);

   String getProperty(String key);

   void println(String s);

   Connection getConnection();

   void setConnection(Connection connection);

   void disconnect();

   void error(String s);

   void error(Throwable t);

   void result(List<ProcessedCommand> commands, String result, boolean isError);

   void refreshProperties();

   CommandBuffer getCommandBuffer();

   CommandRegistry getCommandRegistry();

   IOAdapter getOutputAdapter();

   void setOutputAdapter(IOAdapter outputAdapter);

   void execute();

   void execute(CommandBuffer commandBuffer);

}
