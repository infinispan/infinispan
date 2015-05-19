package org.infinispan.remoting.transport.jgroups;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.commons.util.Util.*;
import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;

import java.io.NotSerializableException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.jcip.annotations.GuardedBy;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.context.Flag;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
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
import org.jgroups.blocks.GroupRequest;
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

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
   public static final RspList<Object> EMPTY_RESPONSES_LIST = new RspList<>();

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
      return invokeRemoteCommands(recipients, command, mode, timeout, filter, deliverOrder, ignoreLeavers);
   }

   /**
    * @param recipients Must <b>not</b> contain self.
    */
   public RspList<Object> invokeRemoteCommands(final List<Address> recipients, final ReplicableCommand command,
                                               final ResponseMode mode, final long timeout, final RspFilter filter,
                                               final DeliverOrder deliverOrder, final boolean ignoreLeavers) throws InterruptedException {
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

   /**
    * Runs a command on each node for each entry in the provided map.
    * NOTE: if ignoreLeavers is true and the node is suspected while executing this
    * method will return a RspList containing for that node a value of ExceptionResponse
    * containing the SuspectException
    *
    * @param commands The commands and where to run them
    * @param mode The response mode to determine how many members must return
    * @param timeout How long to wait before timing out
    * @param oob Whether these should be submitted using out of band thread pool
    * @param ignoreLeavers Whether to ignore leavers.  If this is true and a node leaves
    *        it will send back a response of SuspectException
    * @return The responses that came back in the provided time
    * @throws InterruptedException
    */
   public RspList<Object> invokeRemoteCommands(final Map<Address, ReplicableCommand> commands, final ResponseMode mode, final long timeout,
                                               final boolean oob, final boolean ignoreLeavers) throws InterruptedException {
      try {
         return processCalls(commands, timeout, mode,
               req_marshaller, this, oob, ignoreLeavers);
      } catch (InterruptedException e) {
         throw e;
      } catch (SuspectedException e) {
         throw new SuspectException("One of the nodes " + commands.keySet() + " was suspected", e);
      } catch (org.jgroups.TimeoutException e) {
         // TODO consider ignoreTimeout flag
         throw new TimeoutException("One of the nodes " + commands.keySet() + " timed out", e);
      } catch (Exception e) {
         throw rewrapAsCacheException(e);
      }
   }

   /**
    * @deprecated use instead {@link #invokeRemoteCommand(org.jgroups.Address, org.infinispan.commands.ReplicableCommand,
    * org.jgroups.blocks.ResponseMode, long, org.infinispan.remoting.inboundhandler.DeliverOrder, boolean)}
    */
   @Deprecated
   public Response invokeRemoteCommand(final Address recipient, final ReplicableCommand command, final ResponseMode mode,
                                       final long timeout, final boolean oob) throws InterruptedException {
      return invokeRemoteCommand(recipient, command, mode, timeout, oob ? DeliverOrder.NONE : DeliverOrder.PER_SENDER);
   }

   public Response invokeRemoteCommand(final Address recipient, final ReplicableCommand command, final ResponseMode mode,
                                       final long timeout, final DeliverOrder deliverOrder) throws InterruptedException {
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

   /**
    * @deprecated use instead {@link #broadcastRemoteCommands(org.infinispan.commands.ReplicableCommand,
    * org.jgroups.blocks.ResponseMode, long, org.jgroups.blocks.RspFilter, org.infinispan.remoting.inboundhandler.DeliverOrder,
    * boolean, boolean)}
    */
   @Deprecated
   public RspList<Object> broadcastRemoteCommands(ReplicableCommand command, ResponseMode mode, long timeout,
                                                  boolean oob, RspFilter filter,
                                                  boolean ignoreLeavers, boolean totalOrder)
         throws InterruptedException {
      DeliverOrder deliverOrder = DeliverOrder.PER_SENDER;
      if (totalOrder) {
         deliverOrder = DeliverOrder.TOTAL;
      } else if (oob) {
         deliverOrder = DeliverOrder.NONE;
      }
      return broadcastRemoteCommands(command, mode, timeout, filter, deliverOrder, ignoreLeavers);
   }

   public RspList<Object> broadcastRemoteCommands(ReplicableCommand command, ResponseMode mode, long timeout,
                                                  RspFilter filter, DeliverOrder deliverOrder,
                                                  boolean ignoreLeavers) throws InterruptedException {
      return invokeRemoteCommands(null, command, mode, timeout, filter, deliverOrder, ignoreLeavers);
   }

   private boolean containsOnlyNulls(RspList<Object> l) {
      for (Rsp<Object> r : l.values()) {
         if (r.getValue() != null || r.hasException() || !r.wasReceived() || r.wasSuspected()) return false;
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
            if (cmd == null)
               throw new NullPointerException("Unable to execute a null command!  Message was " + req);
            if (req.getSrc() instanceof SiteAddress) {
               executeCommandFromRemoteSite(cmd, req, response);
            } else {
               executeCommandFromLocalCluster(cmd, req, response);
            }
         } catch (InterruptedException e) {
            log.shutdownHandlingCommand(cmd);
            reply(response, new ExceptionResponse(new CacheException("Cache is shutting down")), cmd);
         } catch (IllegalLifecycleStateException e) {
            if (trace) log.trace("Ignoring command unmarshalling error during shutdown");
            // If this wasn't a CacheRpcCommand, it means the channel is already stopped, and the response won't matter
            reply(response, CacheNotFoundResponse.INSTANCE, cmd);
         } catch (Throwable x) {
            if (cmd == null)
               log.errorUnMarshallingCommand(x);
            else
               log.exceptionHandlingCommand(cmd, x);
            reply(response, new ExceptionResponse(new CacheException("Problems invoking command.", x)), cmd);
         }
      } else {
         reply(response, null, null);
      }
   }

   private void executeCommandFromRemoteSite(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      SiteAddress siteAddress = (SiteAddress) req.getSrc();
      ((XSiteReplicateCommand) cmd).setOriginSite(siteAddress.getSite());
      handler.handleFromRemoteSite(siteAddress.getSite(), (XSiteReplicateCommand) cmd, new Reply() {
         @Override
         public void reply(Object returnValue) {
            CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd);
         }
      }, decodeDeliverMode(req));
   }

   private void executeCommandFromLocalCluster(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      handler.handleFromCluster(fromJGroupsAddress(req.getSrc()), cmd, new Reply() {
         @Override
         public void reply(Object returnValue) {
            CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd);
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

   private void reply(org.jgroups.blocks.Response response, Object retVal, ReplicableCommand command) {
      if (response != null) {
         if (trace) log.tracef("About to send back response %s for command %s", retVal, command);
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
         opts.setExclusionList(card.local_addr);

         Message message = constructMessage(buf, null, mode, rsvp, deliverOrder);
         retval = card.castMessage(dests, message,opts);
      } else {
         RequestOptions opts = new RequestOptions(mode, timeout, true, filter);

         //Only the commands in total order must be received...
         //For correctness, ispn doesn't need their own message, so remove it from the dests collection
         if (dests.contains(card.local_addr)) {
            throw new IllegalArgumentException("Local address is not allowed in the recipients list at this point");
         }

         if (dests.isEmpty()) return EMPTY_RESPONSES_LIST;
         buf = marshallCall(marshaller, command);
         Message msg = constructMessage(buf, null, mode, rsvp, deliverOrder);

         if (mode != ResponseMode.GET_NONE) {
            // A SYNC call that needs to go everywhere (with or without a filter)
            GroupRequest<Object> request = card.cast(dests, msg, opts, true);
            retval = request != null ? request.get() : EMPTY_RESPONSES_LIST;
         } else {
            // An ASYNC call.  We don't care about responses.
            card.cast(dests, msg, opts, false);
            retval = EMPTY_RESPONSES_LIST;
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

   private static RspList<Object> processCalls(Map<Address, ReplicableCommand> commands, long timeout,
                                               ResponseMode mode, Marshaller marshaller, CommandAwareRpcDispatcher card,
                                               boolean oob, boolean ignoreLeavers) throws Exception {
      if (trace) log.tracef("Replication task sending %s with response mode %s", commands, mode);

      if (commands.isEmpty()) return EMPTY_RESPONSES_LIST;

      RequestOptions opts = new RequestOptions(mode, timeout);
      //opts.setExclusionList(card.getChannel().getAddress());

      Map<Address, Future<Object>> futures = new HashMap<Address, Future<Object>>(commands.size());
      RspList<Object> retval = new RspList<>();

      for (Map.Entry<Address, ReplicableCommand> cmd : commands.entrySet()) {
         Buffer buf = marshallCall(marshaller, cmd.getValue());
         Address dest = cmd.getKey();
         boolean rsvp = isRsvpCommand(cmd.getValue());
         futures.put(dest, card.sendMessageWithFuture(constructMessage(buf, dest, oob, mode, rsvp, false), opts));
      }

      // a get() on each future will block till that call completes.
      TimeService timeService = card.timeService;
      long waitTime = timeService.expectedEndTime(timeout, MILLISECONDS);
      for (Map.Entry<Address, Future<Object>> entry : futures.entrySet()) {
         Address target = entry.getKey();
         try {
            retval.addRsp(target, entry.getValue().get(timeService.remainingTime(waitTime, MILLISECONDS), MILLISECONDS));
         } catch (java.util.concurrent.TimeoutException te) {
            throw new TimeoutException(formatString("Timed out after %s waiting for a response from %s",
                  prettyPrintTime(timeout), target));
         } catch (ExecutionException e) {
            if (ignoreLeavers && e.getCause() instanceof SuspectedException) {
               retval.addRsp(target, new ExceptionResponse((SuspectedException) e.getCause()));
            } else {
               throw wrapThrowableInException(e.getCause());
            }
         }
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
}

