/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.horizon.commands.remote;

import org.horizon.commands.DataCommand;
import org.horizon.commands.ReplicableCommand;
import org.horizon.container.DataContainer;
import org.horizon.context.InvocationContext;
import org.horizon.interceptors.InterceptorChain;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Issues a clustered get call, for use primarily by the {@link ClusteredCacheLoader}.  This is not a {@link
 * org.horizon.commands.VisitableCommand} and hence not passed up the {@link org.horizon.interceptors.base.CommandInterceptor}
 * chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 1.0
 */
public class ClusteredGetCommand implements ReplicableCommand {
   public static final byte METHOD_ID = 22;

   private DataCommand dataCommand;
   private DataContainer dataContainer;
   private InterceptorChain interceptorChain;

   private static final Log log = LogFactory.getLog(ClusteredGetCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   public ClusteredGetCommand(boolean unused, DataCommand dataCommand) {
      this.dataCommand = dataCommand;
   }

   public ClusteredGetCommand() {
   }

   public void initialize(DataContainer dataContainer, InterceptorChain interceptorChain) {
      this.dataContainer = dataContainer;
      this.interceptorChain = interceptorChain;
   }

   /**
    * Invokes a {@link DataCommand} on a remote cache and returns results.
    *
    * @param context invocation context, ignored.
    * @return a List containing 2 elements: a boolean, (true or false) and a value (Object) which is the result of
    *         invoking a remote get specified by {@link #getDataCommand()}.
    */
   public Object perform(InvocationContext context) throws Throwable {
      if (trace)
         log.trace("Clustered Get called with param: " + dataCommand);

      Object callResults = null;
      try {
         InvocationContext ctx = interceptorChain.getInvocationContext();
         ctx.setOriginLocal(false);
         // very hacky to be calling this command directly.
         callResults = dataCommand.perform(ctx);
         boolean found = true; // TODO: Revisit this!!
         if (trace) log.trace("Got result " + callResults + ", found=" + found);
         if (found && callResults == null) callResults = createEmptyResults();
      }
      catch (Exception e) {
         log.warn("Problems processing clusteredGet call", e);
      }

      List<Object> results = new ArrayList<Object>(2);
      if (callResults != null) {
         results.add(true);
         results.add(callResults);
      } else {
         results.add(false);
         results.add(null);
      }
      return results;
   }

   public byte getCommandId() {
      return METHOD_ID;
   }

   /**
    * Creates an empty Collection class based on the return type of the method called.
    */
   private Object createEmptyResults() {
      return null;
   }

   public DataCommand getDataCommand() {
      return dataCommand;
   }

   public Object[] getParameters() {
      return new Object[]{dataCommand};  //To change body of implemented methods use File | Settings | File Templates.
   }

   public void setParameters(int commandId, Object[] args) {
      dataCommand = (DataCommand) args[0];
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusteredGetCommand that = (ClusteredGetCommand) o;

      return !(dataCommand != null ? !dataCommand.equals(that.dataCommand) : that.dataCommand != null);
   }

   @Override
   public int hashCode() {
      int result;
      result = (dataCommand != null ? dataCommand.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "ClusteredGetCommand{" +
            "dataCommand=" + dataCommand +
            '}';
   }
}
