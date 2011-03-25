/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.commands.read;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DistributedExecuteCommand<V> extends AbstractQueuedSynchronizer implements VisitableCommand {

   public static final int COMMAND_ID = 19;
   
   private static final Log log = LogFactory.getLog(DistributedExecuteCommand.class);

   private static final long serialVersionUID = -7828117401763700385L;

   /** State value representing that task is running */
   protected static final int RUNNING = 1;

   /** State value representing that task ran */
   protected static final int RAN = 2;

   /** State value representing that task ran remotely and that result is available */
   protected static final int RAN_FUTURE = 3;

   /** State value representing that task was canceled */
   protected static final int CANCELLED = 4;

   protected Cache cache;

   protected Set<Object> keys;

   /** The underlying callable */
   protected Callable<V> callable;

   /** The result to return from get() */
   protected V result;

   /** The exception to throw from get() */
   protected Throwable exception;

   /** The result to return from get() if future was used */
   protected Future<V> resultAsFuture;

   /**
    * The thread running task. When nulled after set/cancel, this indicates that the results are
    * accessible. Must be volatile, to ensure visibility upon completion.
    */
   protected transient volatile Thread runner;

   public DistributedExecuteCommand(Collection<Object> inputKeys, Callable<V> callable) {
      if (inputKeys == null || inputKeys.isEmpty())
         this.keys = Collections.emptySet();
      else
         this.keys = new HashSet<Object>(inputKeys);
      this.callable = callable;
   }

   public DistributedExecuteCommand() {
      this(null, null);
   }

   public void init(Cache cache) {
      this.cache = cache;
   }

   private boolean ranOrCancelled(int state) {
      return (state & (RAN | CANCELLED | RAN_FUTURE)) != 0;
   }

   /**
    * Implements AQS base acquire to succeed if ran or cancelled
    */
   protected int tryAcquireShared(int ignore) {
      return isDone() ? 1 : -1;
   }

   /**
    * Implements AQS base release to always signal after setting final done status by nulling runner
    * thread.
    */
   protected boolean tryReleaseShared(int ignore) {
      runner = null;
      return true;
   }

   public boolean isCancelled() {
      return getState() == CANCELLED;
   }

   public boolean isDone() {
      return ranOrCancelled(getState());
   }

   public V get() throws InterruptedException, ExecutionException {
      acquireSharedInterruptibly(0);
      if (getState() == CANCELLED)
         throw new CancellationException();
      if (exception != null)
         throw new ExecutionException(exception);
      if (getState() == RAN_FUTURE) {
         return retrieveResult();
      }
      return result;
   }

   private V retrieveResult() throws InterruptedException, ExecutionException {
      Object response = resultAsFuture.get();
      if (response instanceof Exception) {
         throw new ExecutionException((Exception) response);
      }
      Map<Address, Response> mapResult = (Map<Address, Response>) response;
      for (Entry<Address, Response> e : mapResult.entrySet()) {
         if (e.getValue() instanceof SuccessfulResponse) {
            return (V) ((SuccessfulResponse) e.getValue()).getResponseValue();
         }
      }
      throw new ExecutionException(new IllegalStateException("No valid response received "
               + response));

   }

   public V get(long nanosTimeout) throws InterruptedException, ExecutionException,
            TimeoutException {
      if (!tryAcquireSharedNanos(0, nanosTimeout))
         throw new TimeoutException();
      if (getState() == CANCELLED)
         throw new CancellationException();
      if (exception != null)
         throw new ExecutionException(exception);

      return retrieveResult();
   }

   void set(V v) {
      for (;;) {
         int s = getState();
         if (s == RAN || s == RAN_FUTURE)
            return;
         if (s == CANCELLED) {
            // aggressively release to set runner to null,
            // in case we are racing with a cancel request
            // that will try to interrupt runner
            releaseShared(0);
            return;
         }
         if (compareAndSetState(s, RAN)) {
            result = v;
            releaseShared(0);
            return;
         }
      }
   }

   public void setWithFuture(Future<V> v) {
      for (;;) {
         int s = getState();
         if (s == RAN_FUTURE || s == RAN) {
            return;
         }
         if (s == CANCELLED) {
            // aggressively release to set runner to null,
            // in case we are racing with a cancel request
            // that will try to interrupt runner
            releaseShared(0);
            return;
         }
         if (compareAndSetState(s, RAN_FUTURE)) {
            resultAsFuture = v;
            releaseShared(0);
            return;
         }
      }
   }

   void setException(Throwable t) {
      for (;;) {
         int s = getState();
         if (s == RAN || s == RAN_FUTURE)
            return;
         if (s == CANCELLED) {
            // aggressively release to set runner to null,
            // in case we are racing with a cancel request
            // that will try to interrupt runner
            releaseShared(0);
            return;
         }
         if (compareAndSetState(s, RAN)) {
            exception = t;
            result = null;
            releaseShared(0);
            return;
         }
      }
   }

   public boolean cancel(boolean mayInterruptIfRunning) {
      for (;;) {
         int s = getState();
         if (ranOrCancelled(s))
            return false;
         if (compareAndSetState(s, CANCELLED))
            break;
      }
      if (mayInterruptIfRunning) {
         Thread r = runner;
         if (r != null)
            r.interrupt();
      }
      releaseShared(0);
      return true;
   }

   public void innerRun() {
      if (!compareAndSetState(0, RUNNING))
         return;
      try {
         runner = Thread.currentThread();
         if (getState() == RUNNING) // recheck after setting thread
            set(callable.call());
         else
            releaseShared(0); // cancel
      } catch (Throwable ex) {
         setException(ex);
      }
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitDistributedExecuteCommand(ctx, this);
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   /**
    * Performs an invalidation on a specified entry
    * 
    * @param ctx
    *           invocation context
    * @return null
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      Callable<V> callable = getCallable();
      if (callable instanceof DistributedCallable<?, ?, ?>) {
         DistributedCallable<Object, Object, Object> dc = (DistributedCallable<Object, Object, Object>) callable;
         dc.setEnvironment(cache, keys);
      }
      innerRun();
      return get();
   }

   private Callable<V> getCallable() {
      return callable;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { keys, callable, result, exception, resultAsFuture };
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      this.keys = (Set<Object>) args[i++];
      this.callable = (Callable) args[i++];
      this.result = (V) args[i++];
      this.exception = (Throwable) args[i++];
      this.resultAsFuture = (Future<V>) args[i++];
   }
}