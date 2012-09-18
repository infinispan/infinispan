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
package org.infinispan.lucene;

import junit.framework.AssertionFailedError;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * SkipIndexingGuaranteed is an interceptor to verify all write operations are using the org.infinispan.context.Flag.SKIP_INDEXING
 * Using SKIP_INDEXING is much lighter than having Infinispan Query need to use class reflection and attempt to reconfigure the Search engine
 * dynamically.
 * 
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2012 Red Hat Inc.
 * @since 5.2
 */
public class SkipIndexingGuaranteed extends CommandInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   protected Object handleDefaultCheckingAssertion(InvocationContext ctx, AbstractFlagAffectedCommand command) throws Throwable {
      if (! command.hasFlag(Flag.SKIP_INDEXING)) {
         throw new AssertionFailedError("A write operation was detected which is not using SKIP_INDEXING flag");
      }
      return super.invokeNextInterceptor(ctx, command);
   }

}
