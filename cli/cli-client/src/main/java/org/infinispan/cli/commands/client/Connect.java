package org.infinispan.cli.commands.client;

import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.AbstractCommand;
import org.infinispan.cli.commands.Argument;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.ConnectionFactory;
import org.infinispan.commons.util.InfinispanCollections;

public class Connect extends AbstractCommand {

   @Override
   public String getName() {
      return "connect";
   }

   @Override
   public boolean isAvailable(Context context) {
      return !context.isConnected();
   }

   @Override
   public void execute(Context context, ProcessedCommand commandLine) {
      if (context.isConnected()) {
         context.disconnect();
      }

      try {
         List<Argument> arguments = commandLine.getArguments();
         String connectionString = arguments.size() > 0 ? arguments.get(0).getValue() : "";
         Connection connection = ConnectionFactory.getConnection(connectionString);
         String password = null;
         if (connection.needsCredentials()) {
            password = new String(context.getOutputAdapter().secureReadln("Password: "));
         }
         connection.connect(password);
         context.setConnection(connection);
      } catch (Exception e) {
         context.error(e);
      }

   }

}
