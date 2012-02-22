/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.remoting.transport.jgroups;

import net.jcip.annotations.GuardedBy;
import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.UpHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.RspFilter;
import org.jgroups.blocks.mux.Muxer;
import org.jgroups.util.Buffer;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.io.NotSerializableException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;
import static org.infinispan.util.Util.*;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
   private ExecutorService asyncExecutor;
   private final InboundInvocationHandler inboundInvocationHandler;
   private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean FORCE_MCAST = Boolean.getBoolean("infinispan.unsafe.force_multicast");
   private final JGroupsTransport transport;

   public CommandAwareRpcDispatcher(Channel channel,
                                    JGroupsTransport transport,
                                    ExecutorService asyncExecutor,
                                    InboundInvocationHandler inboundInvocationHandler) {
      // MessageDispatcher superclass constructors will call start() so perform all init here
      this.setMembershipListener(transport);
      this.setChannel(channel);
      // If existing up handler is a muxing up handler, setChannel(..) will not have replaced it
      UpHandler handler = channel.getUpHandler();
      if (handler instanceof Muxer<?>) {
         @SuppressWarnings("unchecked")
         Muxer<UpHandler> mux = (Muxer<UpHandler>) handler;
         mux.setDefaultHandler(this.prot_adapter);
      }
      channel.addChannelListener(this);
      this.server_obj = transport;
      this.asyncExecutor = asyncExecutor;
      this.inboundInvocationHandler = inboundInvocationHandler;
      this.transport = transport;
   }

   private boolean isValid(Message req) {
      if (req == null || req.getLength() == 0) {
         log.msgOrMsgBufferEmpty();
         return false;
      }

      return true;
   }

   /**
    * @param recipients Guaranteed not to be null.  Must <b>not</b> contain self.
    */
   public RspList<Object> invokeRemoteCommands(final List<Address> recipients, final ReplicableCommand command, final ResponseMode mode, final long timeout,
                                               final boolean anycasting, final boolean oob, final RspFilter filter, final boolean supportReplay,
                                               boolean asyncMarshalling) throws InterruptedException {
      if (asyncMarshalling) {
         asyncExecutor.submit(new Callable<RspList<Object>>() {
            @Override
            public RspList<Object> call() throws Exception {
               return processCalls(command, recipients == null, supportReplay, timeout, filter, recipients, mode,
                                   req_marshaller, CommandAwareRpcDispatcher.this, oob, anycasting);
            }
         });
         return null; // don't wait for a response!
      } else {
         RspList<Object> response;
         try {
            response = processCalls(command, recipients == null, supportReplay, timeout, filter, recipients, mode,
                                    req_marshaller, this, oob, anycasting);
         } catch (InterruptedException e) {
            throw e;
         } catch (Exception e) {
            throw rewrapAsCacheException(e);
         }
         if (mode == ResponseMode.GET_NONE) return null; // "Traditional" async.
         if (response.isEmpty() || containsOnlyNulls(response))
            return null;
         else
            return response;
      }
   }

   public Response invokeRemoteCommand(final Address recipient, final ReplicableCommand command, final ResponseMode mode,
                                       final long timeout, final boolean oob, final boolean supportReplay,
                                       boolean asyncMarshalling) throws InterruptedException {
      if (asyncMarshalling) {
         asyncExecutor.submit(new Callable<Response>() {

            @Override
            public Response call() throws Exception {
               return processSingleCall(command, supportReplay, timeout, recipient, mode,
                                        req_marshaller, CommandAwareRpcDispatcher.this, oob, transport);
            }
         });
         return null; // don't wait for a response!
      } else {
         Response response;
         try {
            response = processSingleCall(command, supportReplay, timeout, recipient, mode,
                                         req_marshaller, this, oob, transport);
         } catch (InterruptedException e) {
            throw e;
         } catch (Exception e) {
            throw rewrapAsCacheException(e);
         }
         if (mode == ResponseMode.GET_NONE) return null; // "Traditional" async.
         return response;
      }
   }

   public RspList<Object> broadcastRemoteCommands(ReplicableCommand command, ResponseMode mode, long timeout,
                                                  boolean anycasting, boolean oob, RspFilter filter, boolean supportReplay,
                                                  boolean asyncMarshalling) throws InterruptedException {
      return invokeRemoteCommands(null, command, mode, timeout, anycasting, oob, filter, supportReplay, asyncMarshalling);
   }

   private boolean containsOnlyNulls(RspList<Object> l) {
      for (Rsp<Object> r : l.values()) {
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
         ReplicableCommand cmd = null;
         try {
            cmd = (ReplicableCommand) req_marshaller.objectFromBuffer(req.getRawBuffer(), req.getOffset(), req.getLength());
            return executeCommand(cmd, req);
         } catch (InterruptedException e) {
            log.warnf("Shutdown while handling command %s", cmd);
            return new ExceptionResponse(new CacheException("Cache is shutting down"));
         } catch (Throwable x) {
            if (cmd == null)
               log.warnf(x, "Problems unmarshalling remote command from byte buffer");
            else
               log.warnf(x, "Problems invoking command %s", cmd);
            return new ExceptionResponse(new CacheException("Problems invoking command.", x));
         }
      } else {
         return null;
      }
   }

   private Object executeCommand(ReplicableCommand cmd, Message req) throws Throwable {
      if (cmd == null) throw new NullPointerException("Unable to execute a null command!  Message was " + req);
      if (cmd instanceof CacheRpcCommand) {
         if (trace) log.tracef("Attempting to execute command: %s [sender=%s]", cmd, req.getSrc());
         return inboundInvocationHandler.handle((CacheRpcCommand) cmd, fromJGroupsAddress(req.getSrc()));
      } else {
         if (trace) log.tracef("Attempting to execute non-CacheRpcCommand command: %s [sender=%s]", cmd, req.getSrc());
         return cmd.perform(null);
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[Outgoing marshaller: " + req_marshaller + "; incoming marshaller: " + rsp_marshaller + "]";
   }

   private static Message constructMessage(Buffer buf, Address recipient, boolean oob, ResponseMode mode) {
      Message msg = new Message();
      msg.setBuffer(buf);
      if (oob) msg.setFlag(Message.OOB);
      if (oob || mode != ResponseMode.GET_NONE) {
         msg.setFlag(Message.DONT_BUNDLE);
         // This is removed since this optimisation is no longer valid.  See ISPN-1878
         // msg.setFlag(Message.NO_FC);
      }
      if (recipient != null) msg.setDest(recipient);
      return msg;
   }

   private static Buffer marshallCall(Marshaller marshaller, ReplicableCommand command) {
      Buffer buf;
      try {
         buf = marshaller.objectToBuffer(command);
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
      return buf;
   }

   private static Response processSingleCall(ReplicableCommand command, boolean supportReplay, long timeout,
                                             Address destination, ResponseMode origMode,
                                             Marshaller marshaller, CommandAwareRpcDispatcher card, boolean oob,
                                             JGroupsTransport transport) throws Exception {
      if (trace) log.tracef("Replication task sending %s to single recipient %s", command, destination);

      // Replay capability requires responses from all members!
      ResponseMode mode = supportReplay ? ResponseMode.GET_ALL : origMode;
      Response retval;
      Buffer buf;
      buf = marshallCall(marshaller, command);
      retval = card.sendMessage(constructMessage(buf, destination, oob, mode),
                                new RequestOptions(mode, timeout));

      // we only bother parsing responses if we are not in ASYNC mode.
      if (trace) log.tracef("Response: %s", retval);
      if (mode != ResponseMode.GET_NONE) {
         if (retval != null) {
            if (!transport.checkResponse(retval, fromJGroupsAddress(destination))) {
               if (trace) log.tracef("Invalid response from %s", destination);
               throw new TimeoutException("Received an invalid response " + retval + " from " + destination);
            }

            if (supportReplay) {
               boolean replay = false;
               if (retval instanceof ExtendedResponse) {
                  ExtendedResponse extended = (ExtendedResponse) retval;
                  replay = extended.isReplayIgnoredRequests();
                  retval = extended.getResponse();
               }

               if (replay) {
                  Message msg = constructMessage(buf, destination, oob, mode);
                  //Since we are making a sync call make sure we don't bundle
                  //See ISPN-192 for more details
                  msg.setFlag(Message.DONT_BUNDLE);

                  if (trace) log.tracef("Replaying message to ignoring sender: %s", destination);
                  Response rv2 = card.sendMessage(msg, new RequestOptions(ResponseMode.GET_ALL, timeout, false));
                  if (rv2 != null) retval = rv2;
               }
            }
         }
         return retval;
      } else {
         return null;
      }
   }

   private static RspList<Object> processCalls(ReplicableCommand command, boolean broadcast, boolean supportReplay, long timeout,
                                               RspFilter filter, List<Address> dests, ResponseMode origMode,
                                               Marshaller marshaller, CommandAwareRpcDispatcher card, boolean oob, boolean anycasting) throws Exception {
      if (trace) log.tracef("Replication task sending %s to addresses %s", command, dests);

      // Replay capability requires responses from all members!
      ResponseMode mode = supportReplay ? ResponseMode.GET_ALL : origMode;

      RspList<Object> retval = null;
      Buffer buf;
      if (broadcast || FORCE_MCAST) {
         buf = marshallCall(marshaller, command);
         retval = card.castMessage(dests, constructMessage(buf, null, oob, mode),
                                   new RequestOptions(mode, timeout, false, filter));
      } else {
         RequestOptions opts = new RequestOptions(mode, timeout);

         if (dests.isEmpty()) return new RspList<Object>();
         buf = marshallCall(marshaller, command);

         // if at all possible, try not to use JGroups' ANYCAST for now.  Multiple (parallel) UNICASTs are much faster.
         if (filter != null) {
            // This is possibly a remote GET.
            // These UNICASTs happen in parallel using sendMessageWithFuture.  Each future has a listener attached
            // (see FutureCollator) and the first successful response is used.
            FutureCollator futureCollator = new FutureCollator(filter, dests.size(), timeout);
            for (Address a : dests) {
               NotifyingFuture<Object> f = card.sendMessageWithFuture(constructMessage(buf, a, oob, mode), opts);
               futureCollator.watchFuture(f, a);
            }
            retval = futureCollator.getResponseList();
         } else if (mode == ResponseMode.GET_ALL) {
            // A SYNC call that needs to go everywhere
            Map<Address, Future<Object>> futures = new HashMap<Address, Future<Object>>(dests.size());

            for (Address dest : dests)
               futures.put(dest, card.sendMessageWithFuture(constructMessage(buf, dest, oob, mode), opts));

            retval = new RspList<Object>();

            // a get() on each future will block till that call completes.
            for (Map.Entry<Address, Future<Object>> entry : futures.entrySet()) {
               try {
                  retval.addRsp(entry.getKey(), entry.getValue().get(timeout, MILLISECONDS));
               } catch (java.util.concurrent.TimeoutException te) {
                  throw new TimeoutException(formatString("Timed out after %s waiting for a response from %s",
                                                          prettyPrintTime(timeout), entry.getKey()));
               }
            }
         } else if (mode == ResponseMode.GET_NONE) {
            // An ASYNC call.  We don't care about responses.
            for (Address dest : dests) card.sendMessage(constructMessage(buf, dest, oob, mode), opts);
         }
      }

      // we only bother parsing responses if we are not in ASYNC mode.
      if (mode != ResponseMode.GET_NONE) {

         if (trace) log.tracef("Responses: %s", retval);

         // a null response is 99% likely to be due to a marshalling problem - we throw a NSE, this needs to be changed when
         // JGroups supports http://jira.jboss.com/jira/browse/JGRP-193
         // the serialization problem could be on the remote end and this is why we cannot catch this above, when marshalling.
         if (retval == null)
            throw new NotSerializableException("RpcDispatcher returned a null.  This is most often caused by args for "
                                                     + command.getClass().getSimpleName() + " not being serializable.");

         if (supportReplay) {
            boolean replay = false;
            List<Address> ignorers = new LinkedList<Address>();
            for (Map.Entry<Address, Rsp<Object>> entry : retval.entrySet()) {
               Object value = entry.getValue().getValue();
               if (value instanceof RequestIgnoredResponse) {
                  ignorers.add(entry.getKey());
               } else if (value instanceof ExtendedResponse) {
                  ExtendedResponse extended = (ExtendedResponse) value;
                  replay |= extended.isReplayIgnoredRequests();
                  entry.getValue().setValue(extended.getResponse());
               }
            }

            if (replay && !ignorers.isEmpty()) {
               Message msg = constructMessage(buf, null, oob, mode);
               //Since we are making a sync call make sure we don't bundle
               //See ISPN-192 for more details
               msg.setFlag(Message.DONT_BUNDLE);

               RequestOptions opts = new RequestOptions(ResponseMode.GET_ALL, timeout, anycasting, filter);
               RspList<Object> responses = card.castMessage(ignorers, msg, opts);
               if (responses != null)
                  retval.putAll(responses);
            }
         }
      }
      return retval;
   }

   static class SenderContainer {
      final Address address;
      volatile boolean processed = false;

      SenderContainer(Address address) {
         this.address = address;
      }

      @Override
      public String toString() {
         return "Sender{" +
               "address=" + address +
               ", responded=" + processed +
               '}';
      }
   }

   final static class FutureCollator implements FutureListener<Object> {
      final RspFilter filter;
      final Map<Future<Object>, SenderContainer> futures = new HashMap<Future<Object>, SenderContainer>(4);
      final long timeout;
      @GuardedBy("this")
      private RspList<Object> retval;
      @GuardedBy("this")
      private Exception exception;
      @GuardedBy("this")
      private int expectedResponses;

      FutureCollator(RspFilter filter, int expectedResponses, long timeout) {
         this.filter = filter;
         this.expectedResponses = expectedResponses;
         this.timeout = timeout;
      }

      public void watchFuture(NotifyingFuture<Object> f, Address address) {
         futures.put(f, new SenderContainer(address));
         f.setListener(this);
      }

      public synchronized RspList<Object> getResponseList() throws Exception {
         while (expectedResponses > 0 && retval == null) {
            try {
               this.wait(timeout);
            } catch (InterruptedException e) {
               // reset interruption flag
               Thread.currentThread().interrupt();
               expectedResponses = -1;
            }
         }
         // Now we either have the response we need or aren't expecting any more responses - or have run out of time.
         if (retval != null)
            return retval;
         else if (exception != null)
            throw exception;
         else if (expectedResponses == 0)
            throw new RpcException(format("No more valid responses.  Received invalid responses from all of %s", futures.values()));
         else
            throw new TimeoutException(format("Timed out waiting for %s for valid responses from any of %s.", Util.prettyPrintTime(timeout), futures.values()));
      }

      @Override
      @SuppressWarnings("unchecked")
      public synchronized void futureDone(Future<Object> objectFuture) {
         SenderContainer sc = futures.get(objectFuture);
         if (sc.processed) {
            // This can happen - it is a race condition in JGroups' NotifyingFuture.setListener() where a listener
            // could be notified twice.
            if (trace) log.tracef("Not processing callback; already processed callback for sender %s", sc.address);
         } else {
            sc.processed = true;
            Address sender = sc.address;
            boolean done = false;
            try {
               if (retval == null) {
                  Object response = objectFuture.get();
                  if (trace) log.tracef("Received response: %s from %s", response, sender);
                  filter.isAcceptable(response, sender);
                  if (!filter.needMoreResponses()) {
                     retval = new RspList(Collections.singleton(new Rsp(sender, response)));
                     done = true;
                     //TODO cancel other tasks?
                  }
               } else {
                  if (trace) log.tracef("Skipping response from %s since a valid response for this request has already been received", sender);
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
               exception = e;
               if (e.getCause() instanceof org.jgroups.TimeoutException)
                  exception = new TimeoutException("Timeout!", e);
               else if (e.getCause() instanceof Exception)
                  exception = (Exception) e.getCause();
               else
                  exception = new CacheException("Caught a throwable", e.getCause());

               if (log.isDebugEnabled())
                  log.debugf("Caught exception %s from sender %s.  Will skip this response.", exception.getClass().getName(), sender);
               log.trace("Exception caught: ", exception);
            } finally {
               expectedResponses--;
               if (expectedResponses == 0 || done) {
                  this.notify(); //make sure to awake waiting thread, but avoid unnecessary wakeups!
               }
            }
         }
      }
   }
}

