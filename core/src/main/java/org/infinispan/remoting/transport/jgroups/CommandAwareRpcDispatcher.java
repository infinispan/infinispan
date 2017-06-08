package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RspFilter;
import org.jgroups.protocols.relay.SiteAddress;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends MessageDispatcher {
   private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean FORCE_MCAST = SecurityActions.getBooleanProperty("infinispan.unsafe.force_multicast");
   public static final short REPLY_FLAGS_TO_CLEAR = (short) (Message.Flag.RSVP.value() | Message.Flag.INTERNAL.value());
   public static final short REPLY_FLAGS_TO_SET =
         (short) (Message.Flag.NO_FC.value() | Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value());

   private final InboundInvocationHandler handler;
   private final ScheduledExecutorService timeoutExecutor;
   private final TimeService timeService;
   private StreamingMarshaller ispnMarshaller;

   public CommandAwareRpcDispatcher(JChannel channel, JGroupsTransport transport,
                                    InboundInvocationHandler globalHandler, ScheduledExecutorService timeoutExecutor,
                                    TimeService timeService, Executor remoteExecutor, StreamingMarshaller ispnMarshaller) {
      super(channel);
      this.handler = globalHandler;
      this.timeoutExecutor = timeoutExecutor;
      this.timeService = timeService;
      this.ispnMarshaller = ispnMarshaller;

      // MessageDispatcher superclass constructors will call start() so perform all init here
      this.setMembershipListener(transport);
      this.setChannel(channel);
      channel.addChannelListener(this);
      asyncDispatching(true);
      correlator(new CustomRequestCorrelator(prot_adapter, this, local_addr, remoteExecutor, ispnMarshaller));
   }

   @Override
   public void close() {
      // Ensure dispatcher is stopped
      this.stop();
      // We must unregister our listener, otherwise the channel will retain a reference to this dispatcher
      this.channel.removeChannelListener(this);
   }

   private boolean isValid(Message req) {
      if (req == null || req.getLength() == 0) {
         log.msgOrMsgBufferEmpty();
         return false;
      }
      return true;
   }

   /**
    * @param recipients Must <b>not</b> contain self.
    */
   public CompletableFuture<Responses> invokeRemoteCommands(List<Address> recipients, ReplicableCommand command,
                                                            ResponseMode mode, long timeout, RspFilter filter,
                                                            DeliverOrder deliverOrder) {
      CompletableFuture<Responses> future;
      try {
         if (recipients != null && recipients.size() > 1 && mode == ResponseMode.GET_FIRST) {
            future = new CompletableFuture<>();
            // This isn't really documented, but some of our internal code uses timeout = 0 as no timeout.
            long nanoTimeout = timeout > 0 ? TimeUnit.MILLISECONDS.toNanos(timeout) : Long.MAX_VALUE;
            long deadline = timeService.expectedEndTime(nanoTimeout, TimeUnit.NANOSECONDS);
            processCallsStaggered(command, filter, recipients, mode, deliverOrder, future,
                  0, deadline, new Responses(recipients));
         } else {
            future = processCalls(command, recipients == null, timeout, filter, recipients, mode, deliverOrder);
         }
         return future;
      } catch (Exception e) {
         return rethrowAsCacheException(e);
      }
   }

   public SingleResponseFuture invokeRemoteCommand(Address recipient, ReplicableCommand command, ResponseMode mode,
         long timeout, DeliverOrder deliverOrder) {
      SingleResponseFuture future;
      try {
         future = processSingleCall(command, timeout, recipient, mode, deliverOrder);
         return future;
      } catch (Exception e) {
         return rethrowAsCacheException(e);
      }
   }

   private <T> T rethrowAsCacheException(Throwable t) {
      if (t instanceof CacheException) throw (CacheException) t;
      else throw new CacheException(t);
   }

   /**
    * Message contains a Command. Execute it against *this* object and return result.
    */
   @Override
   public void handle(Message req, org.jgroups.blocks.Response response) throws Exception {
      if (isValid(req)) {
         ReplicableCommand cmd = null;
         try {
            cmd = (ReplicableCommand) ispnMarshaller.objectFromByteBuffer(req.getRawBuffer(), req.getOffset(),
                  req.getLength());
            if (cmd == null) throw new NullPointerException("Unable to execute a null command!  Message was " + req);
            if (req.getSrc() instanceof SiteAddress) {
               executeCommandFromRemoteSite(cmd, req, response);
            } else {
               executeCommandFromLocalCluster(cmd, req, response);
            }
         } catch (IllegalLifecycleStateException e) {
            if (trace) log.trace("Ignoring command unmarshalling error during shutdown");
            // If this wasn't a CacheRpcCommand, it means the channel is already stopped, and the response won't matter
            reply(response, CacheNotFoundResponse.INSTANCE, cmd, req);
         } catch (Throwable x) {
            if (cmd == null) log.errorUnMarshallingCommand(x);
            else log.exceptionHandlingCommand(cmd, x);
            reply(response, new ExceptionResponse(new CacheException("Problems invoking command.", x)), cmd, req);
         }
      } else {
         reply(response, null, null, req);
      }
   }

   private void executeCommandFromRemoteSite(final ReplicableCommand cmd, final Message req,
         final org.jgroups.blocks.Response response) {
      SiteAddress siteAddress = (SiteAddress) req.getSrc();
      ((XSiteReplicateCommand) cmd).setOriginSite(siteAddress.getSite());
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd, req);
      handler.handleFromRemoteSite(siteAddress.getSite(), (XSiteReplicateCommand) cmd, reply, decodeDeliverMode(req));
   }

   private void executeCommandFromLocalCluster(final ReplicableCommand cmd, final Message req,
         final org.jgroups.blocks.Response response) {
      handler.handleFromCluster(fromJGroupsAddress(req.getSrc()), cmd,
            returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd, req),
            decodeDeliverMode(req));
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

   private static void encodeDeliverMode(RequestOptions request, DeliverOrder deliverOrder) {
      switch (deliverOrder) {
         case TOTAL:
            request.setFlags(Message.Flag.OOB.value());
            break;
         case PER_SENDER:
            request.setFlags(Message.Flag.NO_TOTAL_ORDER.value()).setTransientFlags(Message.TransientFlag.DONT_LOOPBACK.value());
            break;
         case NONE:
            request.setFlags((short) (Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value())).setTransientFlags(
                  Message.TransientFlag.DONT_LOOPBACK.value());
            break;
         default:
            throw new IllegalArgumentException("Unsupported deliver mode " + deliverOrder);
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[Marshaller: " + ispnMarshaller + "]";
   }

   private void reply(org.jgroups.blocks.Response response, Object retVal, ReplicableCommand command, Message req) {
      if (response != null) {
         if (trace) log.tracef("About to send back response %s for command %s", retVal, command);
         Buffer rsp_buf;
         boolean is_exception = false;
         try {
            ByteBuffer bytes = ispnMarshaller.objectToBuffer(retVal);
            rsp_buf = new Buffer(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
         } catch (Throwable t) {
            try {
               // this call should succeed (all exceptions are serializable)
               ByteBuffer bytes = ispnMarshaller.objectToBuffer(t);
               rsp_buf = new Buffer(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
               is_exception = true;
            } catch (IllegalLifecycleStateException tt) {
               return;
            } catch (Throwable tt) {
               log.errorMarshallingObject(tt, retVal);
               return;
            }
         }

         // Always set the NO_FC flag
         short flags = (short) (req.getFlags() | REPLY_FLAGS_TO_SET & ~REPLY_FLAGS_TO_CLEAR);
         Message rsp = req.makeReply().setFlag(flags).setBuffer(rsp_buf);
         if (command instanceof ClusteredGetCommand) {
            rsp.clearFlag(Message.Flag.OOB);
         }

         //exceptionThrown is always false because the exceptions are wrapped in an ExceptionResponse
         try {
            response.send(rsp, is_exception);
         } catch (Throwable t) {
            if (channel.isConnected()) {
               log.errorSendingResponse(command);
            }
         }
      }
   }

   static RequestOptions constructRequestOptions(ResponseMode mode, boolean rsvp, DeliverOrder deliverOrder,
                                                 long timeout, boolean noRelay) {
      RequestOptions options = new RequestOptions(mode, timeout);
      if (noRelay) {
         options.setFlags(Message.Flag.NO_RELAY.value());
      }
      encodeDeliverMode(options, deliverOrder);
      return rsvp ? options.setFlags(Message.Flag.RSVP.value()) : options;
   }

   Buffer marshallCall(ReplicableCommand command) {
      try {
         ByteBuffer bytes = ispnMarshaller.objectToBuffer(command);
         return new Buffer(bytes.getBuf(), bytes.getOffset(), bytes.getLength());
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
   }

   protected SingleResponseFuture processSingleCall(ReplicableCommand command, long timeout, Address destination,
         ResponseMode mode, DeliverOrder deliverOrder) throws Exception {
      if (trace) log.tracef("Replication task sending %s to single recipient %s with response mode %s", command,
            destination, mode);
      boolean rsvp = isRsvpCommand(command);

      // Replay capability requires responses from all members!
      Buffer buf = marshallCall(command);
      RequestOptions options = constructRequestOptions(mode, rsvp, deliverOrder, timeout, true);
      CompletableFuture<Response> request = sendMessageWithFuture(destination, buf, options);
      if (mode == ResponseMode.GET_NONE) return null;

      SingleResponseFuture retval = new SingleResponseFuture(request);
      if (timeout > 0 && !retval.isDone()) {
         ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(retval, timeout, TimeUnit.MILLISECONDS);
         retval.setTimeoutFuture(timeoutFuture);
      }
      return retval;
   }

   private void processCallsStaggered(ReplicableCommand command, RspFilter filter, List<Address> dests,
                                      ResponseMode mode, DeliverOrder deliverOrder,
                                      CompletableFuture<Responses> theFuture, int destIndex,
                                      long deadline, Responses rsps)
         throws Exception {
      if (destIndex == dests.size())
         return;

      Address dest = dests.get(destIndex);
      CompletableFuture<Rsp<Response>> subFuture = processSingleCall(command, -1, dest, mode, deliverOrder);
      if (subFuture != null) {
         subFuture.whenComplete((rsp, throwable) -> {
            if (throwable != null) {
               // We should never get here, any remote exception will be in the Rsp
               theFuture.completeExceptionally(throwable);
            }
            rsps.addResponse(dest, rsp);
            // It is possible that a response has not been received as the node got suspected. Ignore such response
            // (either all nodes are suspected or we get a more meaningful response from another node).
            if (rsp.wasReceived() && (filter == null || filter.isAcceptable(rsp.hasException() ? rsp.getException() : rsp.getValue(), dest))) {
               // We got an acceptable response
               if (trace) log.tracef("Got acceptable response: " + rsps);
               theFuture.complete(rsps);
            } else {
               // We only give up after we've received invalid responses from all recipients,
               // even after the stagger timeout expired for them.
               if (!rsps.isMissingResponses()) {
                  // This was the last response, need to complete the future
                  if (trace) log.tracef("No missing responses: " + rsps);
                  theFuture.complete(rsps);
               } else {
                  // The response was not acceptable, complete the timeout future to start the next request
                  staggeredProcessNext(command, filter, dests, mode, deliverOrder, theFuture, destIndex, deadline,
                        rsps);
               }
            }
         });
         if (!subFuture.isDone()) {
            // If this is the last recipient, schedule a timeout task to cancel the request at the deadline
            // Otherwise, schedule a timeout task to send a staggered request to the next recipient
            long delayNanos = timeService.remainingTime(deadline, TimeUnit.NANOSECONDS);
            if (destIndex < dests.size() - 1) {
               delayNanos = delayNanos / 10 / dests.size();
            }
            ScheduledFuture<?> timeoutTask = timeoutExecutor.schedule(
                  () -> staggeredProcessNext(command, filter, dests, mode, deliverOrder, theFuture, destIndex, deadline,
                        rsps), delayNanos, TimeUnit.NANOSECONDS);
            theFuture.whenComplete((rsps1, throwable) -> timeoutTask.cancel(false));
         }
      } else {
         staggeredProcessNext(command, filter, dests, mode, deliverOrder, theFuture, destIndex, deadline, rsps);
      }
   }

   private void staggeredProcessNext(ReplicableCommand command, RspFilter filter, List<Address> dests,
                                     ResponseMode mode, DeliverOrder deliverOrder,
                                     CompletableFuture<Responses> theFuture, int destIndex, long deadline,
                                     Responses rsps) {
      if (theFuture.isDone()) {
         return;
      }
      if (timeService.isTimeExpired(deadline)) {
         rsps.setTimedOut();
         if (trace) log.tracef("All requests timed out: " + rsps);
         theFuture.complete(rsps);
         return;
      }
      try {
         processCallsStaggered(command, filter, dests, mode, deliverOrder, theFuture, destIndex + 1, deadline, rsps);
      } catch (Exception e) {
         // We should never get here, any remote exception will be in the Rsp
         theFuture.completeExceptionally(e);
      }
   }

   private CompletableFuture<Responses> processCalls(ReplicableCommand command, boolean broadcast, long timeout,
                                                     RspFilter filter, List<Address> dests, ResponseMode mode,
                                                     DeliverOrder deliverOrder) throws Exception {
      if (trace) log.tracef("Replication task sending %s to addresses %s with response mode %s", command, dests, mode);
      boolean rsvp = isRsvpCommand(command);

      Buffer buf = marshallCall(command);
      RequestOptions opts = constructRequestOptions(mode, rsvp, deliverOrder, timeout, true);
      Collection<Address> realDest = dests;
      if (deliverOrder == DeliverOrder.TOTAL) {
         opts.anycasting(true).useAnycastAddresses(true);
      } else if (broadcast || FORCE_MCAST) {
         opts.anycasting(false);
         realDest = null; //broadcast
      } else {
         opts.anycasting(true).setUseAnycastAddresses(false);
      }
      opts.rspFilter(filter);

      GroupRequest<Response> request = cast(realDest, buf, opts, false);
      if (mode == ResponseMode.GET_NONE) return null;
      RspListFuture retval = new RspListFuture(dests, request);
      if (timeout > 0 && !retval.isDone()) {
         ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(retval, timeout, TimeUnit.MILLISECONDS);
         retval.setTimeoutFuture(timeoutFuture);
      }
      return retval;
   }

   static boolean isRsvpCommand(ReplicableCommand command) {
      return command instanceof FlagAffectedCommand &&
            ((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.GUARANTEED_DELIVERY);
   }

   public StreamingMarshaller getIspnMarshaller() {
      return ispnMarshaller;
   }

   public void setIspnMarshaller(StreamingMarshaller ispnMarshaller) {
      this.ispnMarshaller = ispnMarshaller;
   }

}
