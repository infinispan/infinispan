/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.lock;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Interceptor that can selectively fail or skip executing commands.
 *
 * The executor is controlled through a series of {@code Action}s.
 * Each action represents a number of executions, skips or failures for a given command type.
 * The interceptor will match the actions in order, so if the first action has a count of
 * {@code Integer.MAX_VALUE} the rest of the actions will practically never match.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 */
class FailInterceptor extends CommandInterceptor {

   private enum ActionType {
      EXEC, SKIP, FAIL
   }

   private static class Action {
      public Class commandClass;
      public ActionType type;
      public Object returnValue;
      public int count;

      private Action(ActionType type, Class commandClass, Object returnValue, int count) {
         this.commandClass = commandClass;
         this.type = type;
         this.returnValue = returnValue;
         this.count = count;
      }
   }

   public Queue<Action> actions = new LinkedBlockingQueue<Action>();

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      Action action = actions.peek();
      if (action == null || !command.getClass().equals(action.commandClass))
         return super.handleDefault(ctx, command);

      action.count--;
      if (action.count <= 0)
         actions.poll();

      switch (action.type) {
         case EXEC:
            return super.handleDefault(ctx, command);
         case SKIP:
            getLog().debugf("Skipped executing command %s", command);
            return action.returnValue;
         case FAIL:
            throw new CacheException("Forced failure executing command " + command);
         default:
            throw new CacheException("Unexpected FailInterceptor action type: " + action.type);
      }
   }

   public void failFor(Class<? extends ReplicableCommand> commandClass) {
      failFor(commandClass, Integer.MAX_VALUE);
   }

   public void failFor(Class<? extends ReplicableCommand> commandClass, int failCount) {
      actions.add(new Action(ActionType.FAIL, commandClass, null, failCount));
   }

   public void skipFor(Class<? extends ReplicableCommand> commandClass, Object returnValue) {
      skipFor(commandClass, returnValue, Integer.MAX_VALUE);
   }

   public void skipFor(Class<? extends ReplicableCommand> commandClass, Object returnValue, int skipCount) {
      actions.add(new Action(ActionType.SKIP, commandClass, returnValue, skipCount));
   }

   public void execFor(Class<? extends ReplicableCommand> commandClass) {
      execFor(commandClass, Integer.MAX_VALUE);
   }

   public void execFor(Class<? extends ReplicableCommand> commandClass, int execCount) {
      actions.add(new Action(ActionType.EXEC, commandClass, null, execCount));
   }
}
