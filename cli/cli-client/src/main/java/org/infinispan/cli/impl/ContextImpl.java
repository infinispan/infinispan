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
package org.infinispan.cli.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.infinispan.cli.CommandBuffer;
import org.infinispan.cli.CommandRegistry;
import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.io.IOAdapter;

/**
 *
 * ContextImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ContextImpl implements Context {
   private final CommandBuffer commandBuffer;
   private final CommandRegistry commandRegistry;
   private IOAdapter outputAdapter;

   private Connection connection;
   private boolean quitting;
   private Map<String, String> env = new HashMap<String, String>();

   public ContextImpl(IOAdapter outputAdapter, CommandBuffer commandBuffer) {
      this.commandBuffer = commandBuffer;
      this.outputAdapter = outputAdapter;
      this.commandRegistry = new CommandRegistry();
   }

   @Override
   public void setOutputAdapter(IOAdapter outputAdapter) {
      if (this.outputAdapter!=null) {
         try {
            this.outputAdapter.close();
         } catch (IOException e) {
         }
      }
      this.outputAdapter = outputAdapter;
   }

   @Override
   public boolean isConnected() {
      return connection != null && connection.isConnected();
   }

   @Override
   public boolean isQuitting() {
      return quitting;
   }

   @Override
   public void setQuitting(boolean quitting) {
      this.quitting = quitting;
   }

   @Override
   public void setProperty(String key, String value) {
      env.put(key, value);
   }

   @Override
   public String getProperty(String key) {
      return env.get(key);
   }

   @Override
   public void println(String s) {
      try {
         outputAdapter.println(s);
      } catch (IOException e) {
      }
   }

   @Override
   public void error(String s) {
      try {
         outputAdapter.error(s);
      } catch (IOException e) {
      }
   }

   @Override
   public void error(Throwable t) {
      try {
         outputAdapter.error(t.getMessage()!=null?t.getMessage():t.getClass().getName());
      } catch (IOException e) {
      }
   }

   @Override
   public CommandBuffer getCommandBuffer() {
      return commandBuffer;
   }

   @Override
   public void disconnect() {
      if (isConnected()) {
         try {
            connection.close();
         } catch (IOException e) {
         }
         connection = null;
      }
   }

   @Override
   public void setConnection(Connection connection) {
      if (isConnected()) {
         throw new IllegalStateException("Still connected");
      } else {
         this.connection = connection;
      }
   }

   @Override
   public void execute() {
      try {
         connection.execute(this, commandBuffer);
      } finally {
         commandBuffer.reset();
      }
   }

   @Override
   public void execute(CommandBuffer commandBuffer) {
      connection.execute(this, commandBuffer);
   }

   @Override
   public Connection getConnection() {
      return connection;
   }

   @Override
   public CommandRegistry getCommandRegistry() {
      return commandRegistry;
   }

   @Override
   public IOAdapter getOutputAdapter() {
      return outputAdapter;
   }

   @Override
   public void refreshProperties() {
      setProperty("CONNECTION", connection != null ? connection.toString() : "disconnected");
      setProperty("CONTAINER", connection != null ? connection.getActiveContainer() : "");
      setProperty("CACHE", connection != null ? connection.getActiveCache() : "");
   }

}
