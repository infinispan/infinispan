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
package org.horizon.remoting.transport.jgroups;

import org.horizon.CacheException;
import org.horizon.commands.RPCCommand;
import org.horizon.commands.ReplicableCommand;
import org.horizon.commands.remote.ClusteredGetCommand;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.remoting.InboundInvocationHandler;
import org.horizon.remoting.transport.DistributedSync;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.RspFilter;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.io.NotSerializableException;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 1.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
   protected boolean trace;
   ExecutorService asyncExecutor;
   InboundInvocationHandler inboundInvocationHandler;
   DistributedSync distributedSync;
   long distributedSyncTimeout;
   private Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final RequestIgnoredResponse REQUEST_IGNORED_RESPONSE = new RequestIgnoredResponse();

   public CommandAwareRpcDispatcher() {
   }

   public CommandAwareRpcDispatcher(Channel channel,
                                    JGroupsTransport transport,
                                    ExecutorService asyncExecutor,
                                    InboundInvocationHandler inboundInvocationHandler,
                                    DistributedSync distributedSync, long distributedSyncTimeout) {
      super(channel, transport, transport, transport);
      this.asyncExecutor = asyncExecutor;
      this.inboundInvocationHandler = inboundInvocationHandler;
      this.distributedSync = distributedSync;
      trace = log.isTraceEnabled();
      this.distributedSyncTimeout = distributedSyncTimeout;
   }

   protected boolean isValid(Message req) {
      if (req == null || req.getLength() == 0) {
         log.error("message or message buffer is null");
         return false;
      }

      return true;
   }

   /**
    * Similar to {@link #callRemoteMethods(java.util.Vector, org.jgroups.blocks.MethodCall, int, long, boolean, boolean,
    * org.jgroups.blocks.RspFilter)} except that this version is aware of {@link ReplicableCommand} objects.
    */
   public RspList invokeRemoteCommands(Vector<Address> dests, ReplicableCommand command, int mode, long timeout,
                                       boolean anycasting, boolean oob, RspFilter filter, boolean supportReplay)
         throws NotSerializableException, ExecutionException, InterruptedException {
      ReplicationTask task = new ReplicationTask(command, oob, dests, mode, timeout, anycasting, filter, supportReplay);

      if (mode == GroupRequest.GET_NONE) {
         asyncExecutor.submit(task);
         return null; // don't wait for a response!
      } else {
         RspList response = null;
         try {
            response = task.call();
         } catch (Exception e) {
            throw new CacheException(e);
         }
         if (response.isEmpty() || containsOnlyNulls(response))
            return null;
         else
            return response;
      }
   }

   private boolean containsOnlyNulls(RspList l) {
      for (Rsp r : l.values()) {
         if (r.getValue() != null || !r.wasReceived() || r.wasSuspected()) return false;
      }
      return true;
   }

   /**
    * Message contains a Command. Execute it against *this* object and return result.
    */
   @Override
   public Object handle(Message req) {
      if (isValid(req)) {
         try {
            return executeCommand((RPCCommand) req_marshaller.objectFromByteBuffer(req.getBuffer(), req.getOffset(), req.getLength()), req);
         }
         catch (Throwable x) {
            if (trace) log.trace("Problems invoking command.", x);
            return x;
         }
      } else {
         return null;
      }
   }

   protected Object executeCommand(RPCCommand cmd, Message req) throws Throwable {
      if (cmd == null) throw new NullPointerException("Unable to execute a null command!  Message was " + req);
      if (trace) log.trace("Attempting to execute command: {0} [sender={1}]", cmd, req.getSrc());

      boolean unlock = false;
      try {

         int flushCount = distributedSync.getSyncCount();
         distributedSync.acquireProcessingLock(false, distributedSyncTimeout, MILLISECONDS);
         unlock = true;

         distributedSync.blockUntilReleased(distributedSyncTimeout, MILLISECONDS);

         // If this thread blocked during a NBST flush, then inform the sender
         // it needs to replay ignored messages
         boolean replayIgnored = distributedSync.getSyncCount() != flushCount;

         Object retval;
         try {
            retval = inboundInvocationHandler.handle(cmd);
         } catch (IllegalStateException ise) {
            if (trace) log.trace("Unable to execute command, cache not in a receptive state");
            // cache not in a started state, request replay
            return REQUEST_IGNORED_RESPONSE;
         }

         if (replayIgnored) {
            ExtendedResponse extended = new ExtendedResponse(retval);
            extended.setReplayIgnoredRequests(true);
            return extended;
         } else {

            // Do we really need a response?!?  The caller would only ever expect a response for certain types of
            // commands, such as a ClusteredGet
            if (cmd.isSingleCommand() && cmd.getSingleCommand() instanceof ClusteredGetCommand)
               return retval;
            else
               return null; // saves on serializing a response!
         }
      } finally {
         if (unlock) distributedSync.releaseProcessingLock();
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[Outgoing marshaller: " + req_marshaller + "; incoming marshaller: " + rsp_marshaller + "]";
   }

   private class ReplicationTask implements Callable<RspList> {

      private ReplicableCommand command;
      private boolean oob;
      private Vector<Address> dests;
      private int mode;
      private long timeout;
      private boolean anycasting;
      private RspFilter filter;
      boolean supportReplay = false;

      private ReplicationTask(ReplicableCommand command, boolean oob, Vector<Address> dests,
                              int mode, long timeout,
                              boolean anycasting, RspFilter filter, boolean supportReplay) {
         this.command = command;
         this.oob = oob;
         this.dests = dests;
         this.mode = mode;
         this.timeout = timeout;
         this.anycasting = anycasting;
         this.filter = filter;
         this.supportReplay = supportReplay;
      }

      public RspList call() throws Exception {
         Buffer buf;
         try {
            buf = req_marshaller.objectToBuffer(command);
         }
         catch (Exception e) {
            throw new RuntimeException("Failure to marshal argument(s)", e);
         }

         Message msg = new Message();
         msg.setBuffer(buf);
         if (oob) msg.setFlag(Message.OOB);
         // Replay capability requires responses from all members!
         int mode = supportReplay ? GroupRequest.GET_ALL : this.mode;
         RspList retval = castMessage(dests, msg, mode, timeout, anycasting, filter);
         if (trace) log.trace("responses: {0}", retval);

         // a null response is 99% likely to be due to a marshalling problem - we throw a NSE, this needs to be changed when
         // JGroups supports http://jira.jboss.com/jira/browse/JGRP-193
         // the serialization problem could be on the remote end and this is why we cannot catch this above, when marshalling.
         if (retval == null)
            throw new NotSerializableException("RpcDispatcher returned a null.  This is most often caused by args for "
                  + command.getClass().getSimpleName() + " not being serializable.");

         if (supportReplay) {
            boolean replay = false;
            Vector<Address> ignorers = new Vector<Address>();
            for (Map.Entry<Address, Rsp> entry : retval.entrySet()) {
               Object value = entry.getValue().getValue();
               if (value instanceof RequestIgnoredResponse) {
                  ignorers.add(entry.getKey());
               } else if (value instanceof ExtendedResponse) {
                  ExtendedResponse extended = (ExtendedResponse) value;
                  replay |= extended.isReplayIgnoredRequests();
                  entry.getValue().setValue(extended.getResponse());
               }
            }

            if (replay && ignorers.size() > 0) {
               if (trace)
                  log.trace("Replaying message to ignoring senders: " + ignorers);
               RspList responses = castMessage(ignorers, msg, GroupRequest.GET_ALL, timeout, anycasting, filter);
               if (responses != null)
                  retval.putAll(responses);
            }
         }

         return retval;
      }
   }
}
