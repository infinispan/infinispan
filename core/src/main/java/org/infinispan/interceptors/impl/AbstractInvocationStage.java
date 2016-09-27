package org.infinispan.interceptors.impl;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationStage;

/**
 * @author Dan Berindei
 * @since 9.0
 */
public abstract class AbstractInvocationStage implements InvocationStage {
   InvocationContext ctx;
   VisitableCommand command;

   public AbstractInvocationStage(InvocationContext ctx, VisitableCommand command) {
      this.command = command;
      this.ctx = ctx;
   }

   @Override
   public InvocationStage toInvocationStage(InvocationContext newCtx, VisitableCommand newCommand) {
      if (ctx != newCtx) {
         ctx = newCtx;
      }
      if (command != newCommand) {
         command = newCommand;
      }
      return this;
   }
}
