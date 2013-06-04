/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.commands.client;

import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.AbstractCommand;
import org.infinispan.cli.commands.Argument;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.ConnectionFactory;

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
         connection.connect(context, password);
         context.setConnection(connection);
      } catch (Exception e) {
         context.error(e);
      }

   }

}
