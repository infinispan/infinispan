package org.infinispan.remoting.transport.jgroups;

import net.jcip.annotations.GuardedBy;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.context.Flag;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.Address;
import org.jgroups.AnycastAddress;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.SuspectedException;
import org.jgroups.UpHandler;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.RspFilter;
import org.jgroups.blocks.mux.Muxer;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Buffer;
import org.jgroups.util.FutureListener;
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.io.NotSerializableException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.commons.util.Util.*;
import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {

   private final ExecutorService asyncExecutor;
   private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean FORCE_MCAST = Boolean.getBoolean("infinispan.unsafe.force_multicast");
   private final JGroupsTransport transport;
   private final TimeService timeService;
   private final InboundInvocationHandler handler;

   public CommandAwareRpcDispatcher(Channel channel,
         JGroupsTransport transport,
         ExecutorService asyncExecutor,
         TimeService timeService,
         InboundInvocationHandler globalHandler) {
      this.server_obj = transport;
      this.asyncExecutor = asyncExecutor;
      this.transport = transport;
      this.timeService = timeService;
      this.handler = globalHandler;

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
      asyncDispatching(true);
   }

   @Override
   protected RequestCorrelator createRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr) {
      return new CustomRequestCorrelator(transport, handler, local_addr);
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
    * @deprecated use instead {@link #invokeRemoteCommands(java.util.List, org.infinispan.commands.ReplicableCommand,
    * org.jgroups.blocks.ResponseMode, long, org.jgroups.blocks.RspFilter, org.infinispan.remoting.inboundhandler.DeliverOrder,
    * boolean, boolean)}
    */
   @Deprecated
   public RspList<Object> invokeRemoteCommands(final List<Address> recipients, final ReplicableCommand command, final ResponseMode mode, final long timeout,
                                               final boolean oob, final RspFilter filter,
                                               boolean asyncMarshalling, final boolean ignoreLeavers, final boolean totalOrder) throws InterruptedException {
      DeliverOrder deliverOrder = DeliverOrder.PER_SENDER;
      if (totalOrder) {
         deliverOrder = DeliverOrder.TOTAL;
      } else if (oob) {
         deliverOrder = DeliverOrder.NONE;
      }
      return invokeRemoteCommands(recipients, command, mode, timeout, filter, deliverOrder, asyncMarshalling, ignoreLeavers);
   }

   /**
    * @param recipients Guaranteed not to be null.  Must <b>not</b> contain self.
    */
   public RspList<Object> invokeRemoteCommands(final List<Address> recipients, final ReplicableCommand command,
                                               final ResponseMode mode, final long timeout, final RspFilter filter,
                                               final DeliverOrder deliverOrder, boolean asyncMarshalling,
                                               final boolean ignoreLeavers) throws InterruptedException {
      if (asyncMarshalling) {
         asyncExecutor.submit(new Callable<RspList<Object>>() {
            @Override
            public RspList<Object> call() throws Exception {
               return processCalls(command, recipients == null, timeout, filter, recipients, mode, deliverOrder,
                                   req_marshaller, CommandAwareRpcDispatcher.this, ignoreLeavers);
            }
         });
         return null; // don't wait for a response!
      } else {
         RspList<Object> response;
         try {
            response = processCalls(command, recipients == null, timeout, filter, recipients, mode, deliverOrder,
                                    req_marshaller, this, ignoreLeavers);
         } catch (InterruptedException e) {
            throw e;
         } catch (SuspectedException e) {
            throw new SuspectException("One of the nodes " + recipients + " was suspected", e);
         } catch (org.jgroups.TimeoutException e) {
            throw new TimeoutException("One of the nodes " + recipients + " timed out", e);
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

   /**
    * @deprecated use instead {@link #invokeRemoteCommand(org.jgroups.Address, org.infinispan.commands.ReplicableCommand,
    * org.jgroups.blocks.ResponseMode, long, org.infinispan.remoting.inboundhandler.DeliverOrder, boolean)}
    */
   @Deprecated
   public Response invokeRemoteCommand(final Address recipient, final ReplicableCommand command, final ResponseMode mode,
                                       final long timeout, final boolean oob,
                                       boolean asyncMarshalling) throws InterruptedException {
      return invokeRemoteCommand(recipient, command, mode, timeout, oob ? DeliverOrder.NONE : DeliverOrder.PER_SENDER, asyncMarshalling);
   }

   public Response invokeRemoteCommand(final Address recipient, final ReplicableCommand command, final ResponseMode mode,
                                       final long timeout, final DeliverOrder deliverOrder, boolean asyncMarshalling) throws InterruptedException {
      if (asyncMarshalling) {
         asyncExecutor.submit(new Callable<Response>() {

            @Override
            public Response call() throws Exception {
               return processSingleCall(command, timeout, recipient, mode, deliverOrder,
                                        req_marshaller, CommandAwareRpcDispatcher.this, transport);
            }
         });
         return null; // don't wait for a response!
      } else {
         Response response;
         try {
            response = processSingleCall(command, timeout, recipient, mode, deliverOrder,
                                         req_marshaller, this, transport);
         } catch (InterruptedException e) {
            throw e;
         } catch (SuspectedException e) {
            throw new SuspectException("Node " + recipient + " was suspected", e);
         } catch (org.jgroups.TimeoutException e) {
            throw new TimeoutException("Node " + recipient + " timed out", e);
         } catch (Exception e) {
            throw rewrapAsCacheException(e);
         }
         if (mode == ResponseMode.GET_NONE) return null; // "Traditional" async.
         return response;
      }
   }

   /**
    * @deprecated use instead {@link #broadcastRemoteCommands(org.infinispan.commands.ReplicableCommand,
    * org.jgroups.blocks.ResponseMode, long, org.jgroups.blocks.RspFilter, org.infinispan.remoting.inboundhandler.DeliverOrder,
    * boolean, boolean)}
    */
   @Deprecated
   public RspList<Object> broadcastRemoteCommands(ReplicableCommand command, ResponseMode mode, long timeout,
                                                  boolean oob, RspFilter filter,
                                                  boolean asyncMarshalling, boolean ignoreLeavers, boolean totalOrder)
         throws InterruptedException {
      DeliverOrder deliverOrder = DeliverOrder.PER_SENDER;
      if (totalOrder) {
         deliverOrder = DeliverOrder.TOTAL;
      } else if (oob) {
         deliverOrder = DeliverOrder.NONE;
      }
      return broadcastRemoteCommands(command, mode, timeout, filter, deliverOrder, asyncMarshalling, ignoreLeavers);
   }

   public RspList<Object> broadcastRemoteCommands(ReplicableCommand command, ResponseMode mode, long timeout,
                                                  RspFilter filter, DeliverOrder deliverOrder, boolean asyncMarshalling,
                                                  boolean ignoreLeavers) throws InterruptedException {
      return invokeRemoteCommands(null, command, mode, timeout, filter, deliverOrder, asyncMarshalling,
                                  ignoreLeavers);
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
   public void handle(Message req, org.jgroups.blocks.Response response) throws Exception {
      if (isValid(req)) {
         ReplicableCommand cmd = null;
         try {
            cmd = (ReplicableCommand) req_marshaller.objectFromBuffer(req.getRawBuffer(), req.getOffset(), req.getLength());
            if (cmd == null) throw new NullPointerException("Unable to execute a null command!  Message was " + req);
            if (req.getSrc() instanceof SiteAddress) {
               executeCommandFromRemoteSite(cmd, req, response);
            } else {
               executeCommandFromLocalCluster(cmd, req, response);
            }
         } catch (InterruptedException e) {
            log.shutdownHandlingCommand(cmd);
            reply(response, new ExceptionResponse(new CacheException("Cache is shutting down")));
         } catch (Throwable x) {
            if (cmd == null)
               log.errorUnMarshallingCommand(x);
            else
               log.exceptionHandlingCommand(cmd, x);
            reply(response, new ExceptionResponse(new CacheException("Problems invoking command.", x)));
         }
      } else {
         reply(response, null);
      }
   }

   private void executeCommandFromRemoteSite(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      SiteAddress siteAddress = (SiteAddress) req.getSrc();
      ((XSiteReplicateCommand) cmd).setOriginSite(siteAddress.getSite());
      handler.handleFromRemoteSite(siteAddress.getSite(), (XSiteReplicateCommand) cmd, new Reply() {
         @Override
         public void reply(Object returnValue) {
            CommandAwareRpcDispatcher.this.reply(response, returnValue);
         }
      }, decodeDeliverMode(req));
   }

   private void executeCommandFromLocalCluster(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      handler.handleFromCluster(fromJGroupsAddress(req.getSrc()), cmd, new Reply() {
         @Override
         public void reply(Object returnValue) {
            CommandAwareRpcDispatcher.this.reply(response, returnValue);
         }
      }, decodeDeliverMode(req));
   }

   private static DeliverOrder decodeDeliverMode(Message request) {
      boolean noTotalOrder = request.isFlagSet(Message.Flag.NO_TOTAL_ORDER);
      boolean oob = request.isFlagSet(Message.Flag.OOB);
      if (!noTotalOrder && oob) {
         return DeliverOrder.TOTAL;
      } else if (noTotalOrder && oob) {
         return DeliverOrder.NONE;
      } else if (noTotalOrder) {
         //oob is not set at this point, but the no total order flag should.
         return DeliverOrder.PER_SENDER;
      }
      throw new IllegalArgumentException("Unable to decode message " + request);
   }

   private static void encodeDeliverMode(Message request, DeliverOrder deliverOrder) {
      switch (deliverOrder) {
         case TOTAL:
            request.setFlag(Message.Flag.OOB);
            request.clearFlag(Message.Flag.NO_TOTAL_ORDER);
            break;
         case PER_SENDER:
            request.clearFlag(Message.Flag.OOB);
            request.setFlag(Message.Flag.NO_TOTAL_ORDER);
            break;
         case NONE:
            request.setFlag(Message.Flag.OOB, Message.Flag.NO_TOTAL_ORDER);
            break;
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[Outgoing marshaller: " + req_marshaller + "; incoming marshaller: " + rsp_marshaller + "]";
   }

   private void reply(org.jgroups.blocks.Response response, Object retVal) {
      if (response != null) {
         //exceptionThrown is always false because the exceptions are wrapped in an ExceptionResponse
         response.send(retVal, false);
      }
   }

   protected static Message constructMessage(Buffer buf, Address recipient, boolean oob, ResponseMode mode, boolean rsvp,
                                             boolean totalOrder) {
      DeliverOrder deliverOrder = DeliverOrder.PER_SENDER;
      if (totalOrder) {
         deliverOrder = DeliverOrder.TOTAL;
      } else if (oob) {
         deliverOrder = DeliverOrder.NONE;
      }
      return constructMessage(buf, recipient, mode, rsvp, deliverOrder);
   }

   protected static Message constructMessage(Buffer buf, Address recipient,ResponseMode mode, boolean rsvp,
                                             DeliverOrder deliverOrder) {
      Message msg = new Message();
      msg.setBuffer(buf);
      encodeDeliverMode(msg, deliverOrder);
      //some issues with the new bundler. put back the DONT_BUNDLE flag.
      if (deliverOrder == DeliverOrder.NONE || mode != ResponseMode.GET_NONE) msg.setFlag(Message.Flag.DONT_BUNDLE);
      if (rsvp) msg.setFlag(Message.Flag.RSVP);

      if (recipient != null) msg.setDest(recipient);
      return msg;
   }

   static Buffer marshallCall(Marshaller marshaller, ReplicableCommand command) {
      Buffer buf;
      try {
         buf = marshaller.objectToBuffer(command);
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
      return buf;
   }

   private static Response processSingleCall(ReplicableCommand command, long timeout,
                                             Address destination, ResponseMode mode, DeliverOrder deliverOrder,
                                             Marshaller marshaller, CommandAwareRpcDispatcher card,
                                             JGroupsTransport transport) throws Exception {
      if (trace) log.tracef("Replication task sending %s to single recipient %s with response mode %s", command, destination, mode);
      boolean rsvp = isRsvpCommand(command);

      // Replay capability requires responses from all members!
      Response retval;
      Buffer buf;
      buf = marshallCall(marshaller, command);
      retval = card.sendMessage(constructMessage(buf, destination, mode, rsvp, deliverOrder),
                                new RequestOptions(mode, timeout));

      // we only bother parsing responses if we are not in ASYNC mode.
      if (trace) log.tracef("Response: %s", retval);
      if (mode == ResponseMode.GET_NONE)
         return null;

      if (retval != null) {
         if (!transport.checkResponse(retval, fromJGroupsAddress(destination))) {
            if (trace) log.tracef("Invalid response from %s", destination);
            throw new TimeoutException("Received an invalid response " + retval + " from " + destination);
         }
      }

      return retval;
   }

   private static RspList<Object> processCalls(ReplicableCommand command, boolean broadcast, long timeout,
                                               RspFilter filter, List<Address> dests, ResponseMode mode, DeliverOrder deliverOrder,
                                               Marshaller marshaller, CommandAwareRpcDispatcher card,
                                               boolean ignoreLeavers) throws Exception {
      if (trace) log.tracef("Replication task sending %s to addresses %s with response mode %s", command, dests, mode);
      boolean rsvp = isRsvpCommand(command);

      RspList<Object> retval = null;
      Buffer buf;
      if (deliverOrder == DeliverOrder.TOTAL) {
         buf = marshallCall(marshaller, command);
         Message message = constructMessage(buf, new AnycastAddress(dests), mode, rsvp, deliverOrder);

         retval = card.castMessage(dests, message, new RequestOptions(mode, timeout, false, filter));
      } else if (broadcast || FORCE_MCAST) {
         buf = marshallCall(marshaller, command);
         RequestOptions opts = new RequestOptions(mode, timeout, false, filter);

         //Only the commands in total order must be received...
         //For correctness, ispn doesn't need their own message, so add own address to exclusion list
         opts.setExclusionList(card.getChannel().getAddress());

         retval = card.castMessage(dests, constructMessage(buf, null, mode, rsvp, deliverOrder),opts);
      } else {
         RequestOptions opts = new RequestOptions(mode, timeout);

         //Only the commands in total order must be received...
         opts.setExclusionList(card.getChannel().getAddress());

         if (dests.isEmpty()) return new RspList<>();
         buf = marshallCall(marshaller, command);

         // if at all possible, try not to use JGroups' ANYCAST for now.  Multiple (parallel) UNICASTs are much faster.
         if (filter != null) {
            // This is possibly a remote GET.
            // These UNICASTs happen in parallel using sendMessageWithFuture.  Each future has a listener attached
            // (see FutureCollator) and the first successful response is used.
            FutureCollator futureCollator = new FutureCollator(filter, dests.size(), timeout, card.timeService);
            for (Address a : dests) {
               NotifyingFuture<Object> f = card.sendMessageWithFuture(constructMessage(buf, a, mode, rsvp, deliverOrder), opts);
               futureCollator.watchFuture(f, a);
            }
            retval = futureCollator.getResponseList();
         } else if (mode == ResponseMode.GET_ALL) {
            // A SYNC call that needs to go everywhere
            Map<Address, Future<Object>> futures = new HashMap<>(dests.size());

            for (Address dest : dests)
               futures.put(dest, card.sendMessageWithFuture(constructMessage(buf, dest, mode, rsvp, deliverOrder), opts));

            retval = new RspList<>();

            // a get() on each future will block till that call completes.
            for (Map.Entry<Address, Future<Object>> entry : futures.entrySet()) {
               Address target = entry.getKey();
               try {
                  retval.addRsp(target, entry.getValue().get(timeout, MILLISECONDS));
               } catch (java.util.concurrent.TimeoutException te) {
                  throw new TimeoutException(formatString("Timed out after %s waiting for a response from %s",
                                                          prettyPrintTime(timeout), target));
               } catch (ExecutionException e) {
                  if (ignoreLeavers && e.getCause() instanceof SuspectedException) {
                     log.tracef(formatString("Ignoring node %s that left during the remote call", target));
                  } else {
                     throw wrapThrowableInException(e.getCause());
                  }
               }
            }
         } else if (mode == ResponseMode.GET_NONE) {
            // An ASYNC call.  We don't care about responses.
            for (Address dest : dests) card.sendMessage(constructMessage(buf, dest, mode, rsvp, deliverOrder), opts);
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
      }

      return retval;
   }

   private static Exception wrapThrowableInException(Throwable t) {
      if (t instanceof Exception) {
         return (Exception) t;
      } else {
         return new CacheException(t);
      }
   }

   private static boolean isRsvpCommand(ReplicableCommand command) {
      return command instanceof FlagAffectedCommand
            && ((FlagAffectedCommand) command).hasFlag(Flag.GUARANTEED_DELIVERY);
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
      final Map<Future<Object>, SenderContainer> futures = new HashMap<>(4);
      final long timeout;
      @GuardedBy("this")
      private RspList<Object> retval;
      @GuardedBy("this")
      private Exception exception;
      @GuardedBy("this")
      private int expectedResponses;
      private final TimeService timeService;

      FutureCollator(RspFilter filter, int expectedResponses, long timeout, TimeService timeService) {
         this.filter = filter;
         this.expectedResponses = expectedResponses;
         this.timeout = timeout;
         this.timeService = timeService;
      }

      public void watchFuture(NotifyingFuture<Object> f, Address address) {
         futures.put(f, new SenderContainer(address));
         f.setListener(this);
      }

      public synchronized RspList<Object> getResponseList() throws Exception {
         long expectedEndTime = timeService.expectedEndTime(timeout, MILLISECONDS);
         long waitingTime;
         while (expectedResponses > 0 && retval == null &&
               (waitingTime = timeService.remainingTime(expectedEndTime, MILLISECONDS)) > 0) {
            try {
               this.wait(waitingTime);
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
               Throwable cause = e.getCause();
               if (cause instanceof org.jgroups.SuspectedException) {
                  // Do not set the exception field, RpcException should be thrown if there is no other valid response
                  return;
               } else if (cause instanceof org.jgroups.TimeoutException) {
                  exception = new TimeoutException("Timeout!", e);
               } else if (cause instanceof Exception) {
                  exception = (Exception) cause;
               } else {
                  exception = new CacheException("Caught a throwable", cause);
               }

               if (log.isDebugEnabled())
                  log.debugf("Caught exception from sender %s: %s", sender, exception);
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

