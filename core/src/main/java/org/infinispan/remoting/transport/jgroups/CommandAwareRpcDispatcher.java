package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
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
import org.jgroups.AnycastAddress;
import org.jgroups.Channel;
import org.jgroups.Message;
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
import org.jgroups.util.NotifyingFuture;
import org.jgroups.util.Rsp;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
   private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean FORCE_MCAST = SecurityActions.getBooleanProperty("infinispan.unsafe.force_multicast");
   private static long STAGGER_DELAY_NANOS = TimeUnit.MILLISECONDS.toNanos(
         SecurityActions.getIntProperty("infinispan.stagger.delay", 5));
   public static final short REPLY_FLAGS_TO_CLEAR = (short) (Message.Flag.RSVP.value() | Message.Flag.SCOPED.value()
         | Message.Flag.INTERNAL.value());
   public static final short REPLY_FLAGS_TO_SET =
         (short) (Message.Flag.NO_FC.value() | Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value());

   private final InboundInvocationHandler handler;
   private final ScheduledExecutorService timeoutExecutor;
   private final TimeService timeService;

   public CommandAwareRpcDispatcher(Channel channel, JGroupsTransport transport,
         InboundInvocationHandler globalHandler, ScheduledExecutorService timeoutExecutor,
         TimeService timeService) {
      this.timeService = timeService;
      this.server_obj = transport;
      this.handler = globalHandler;
      this.timeoutExecutor = timeoutExecutor;

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
   public void close() {
      // Ensure dispatcher is stopped
      this.stop();
      // We must unregister our listener, otherwise the channel will retain a reference to this dispatcher
      this.channel.removeChannelListener(this);
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
    * @param recipients Must <b>not</b> contain self.
    */
   public CompletableFuture<Responses> invokeRemoteCommands(List<Address> recipients, ReplicableCommand command,
                                                            ResponseMode mode, long timeout, RspFilter filter,
                                                            DeliverOrder deliverOrder) {
      CompletableFuture<Responses> future;
      try {
         if (recipients != null && mode == ResponseMode.GET_FIRST && STAGGER_DELAY_NANOS > 0) {
            future = new CompletableFuture<>();
            // This isn't really documented, but some of our internal code uses timeout = 0 as no timeout.
            long nanoTimeout = timeout > 0 ? TimeUnit.MILLISECONDS.toNanos(timeout) : Long.MAX_VALUE;
            long deadline = timeService.expectedEndTime(nanoTimeout, TimeUnit.NANOSECONDS);
            processCallsStaggered(command, filter, recipients, mode, deliverOrder, req_marshaller, future,
                  0, deadline, new Responses(recipients));
         } else {
            future = processCalls(command, recipients == null, timeout, filter, recipients, mode, deliverOrder,
                  req_marshaller);
         }
         return future;
      } catch (Exception e) {
         return rethrowAsCacheException(e);
      }
   }

   public SingleResponseFuture invokeRemoteCommand(Address recipient, ReplicableCommand command,
                                                   ResponseMode mode, long timeout,
                                                   DeliverOrder deliverOrder) {
      SingleResponseFuture future;
      try {
         future = processSingleCall(command, timeout, recipient, mode, deliverOrder, req_marshaller);
         return future;
      } catch (Exception e) {
         return rethrowAsCacheException(e);
      }
   }

   public <T> T rethrowAsCacheException(Throwable t) {
      if (t instanceof CacheException)
         throw (CacheException) t;
      else
         throw new CacheException(t);
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
            reply(response, new ExceptionResponse(new CacheException("Cache is shutting down")), cmd, req);
         } catch (IllegalLifecycleStateException e) {
            if (trace) log.trace("Ignoring command unmarshalling error during shutdown");
            // If this wasn't a CacheRpcCommand, it means the channel is already stopped, and the response won't matter
            reply(response, CacheNotFoundResponse.INSTANCE, cmd, req);
         } catch (Throwable x) {
            if (cmd == null)
               log.errorUnMarshallingCommand(x);
            else
               log.exceptionHandlingCommand(cmd, x);
            reply(response, new ExceptionResponse(new CacheException("Problems invoking command.", x)), cmd,
                  req);
         }
      } else {
         reply(response, null, null, req);
      }
   }

   private void executeCommandFromRemoteSite(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      SiteAddress siteAddress = (SiteAddress) req.getSrc();
      ((XSiteReplicateCommand) cmd).setOriginSite(siteAddress.getSite());
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd, req);
      handler.handleFromRemoteSite(siteAddress.getSite(), (XSiteReplicateCommand) cmd, reply, decodeDeliverMode(req));
   }

   private void executeCommandFromLocalCluster(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd, req);
      handler.handleFromCluster(fromJGroupsAddress(req.getSrc()), cmd, reply, decodeDeliverMode(req));
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
            request.setFlag(Message.Flag.OOB.value());
            break;
         case PER_SENDER:
            request.setFlag(Message.Flag.NO_TOTAL_ORDER.value());
            break;
         case NONE:
            request.setFlag((short) (Message.Flag.OOB.value() | Message.Flag.NO_TOTAL_ORDER.value()));
            break;
         default:
            throw new IllegalArgumentException("Unsupported deliver mode " + deliverOrder);
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "[Outgoing marshaller: " + req_marshaller + "; incoming marshaller: " + rsp_marshaller + "]";
   }

   private void reply(org.jgroups.blocks.Response response, Object retVal, ReplicableCommand command,
         Message req) {
      if (response != null) {
         if (trace) log.tracef("About to send back response %s for command %s", retVal, command);
         Buffer rsp_buf;
         boolean is_exception = false;
         try {
            rsp_buf = rsp_marshaller.objectToBuffer(retVal);
         } catch (Throwable t) {
            try {  // this call should succeed (all exceptions are serializable)
               rsp_buf = rsp_marshaller.objectToBuffer(t);
               is_exception = true;
            } catch (Throwable tt) {
               log.errorMarshallingObject(tt, retVal);
               return;
            }
         }

         // Always set the NO_FC flag
         short flags = (short) (req.getFlags() | REPLY_FLAGS_TO_SET & ~REPLY_FLAGS_TO_CLEAR);
         Message rsp = req.makeReply().setFlag(flags).setBuffer(rsp_buf);

         //exceptionThrown is always false because the exceptions are wrapped in an ExceptionResponse
         response.send(rsp, is_exception);
      }
   }

   protected static Message constructMessage(Buffer buf, Address recipient,ResponseMode mode, boolean rsvp,
                                             DeliverOrder deliverOrder) {
      Message msg = new Message();
      msg.setBuffer(buf);
      encodeDeliverMode(msg, deliverOrder);
      //some issues with the new bundler. put back the DONT_BUNDLE flag.
      if (deliverOrder == DeliverOrder.NONE || mode != ResponseMode.GET_NONE) {
         msg.setFlag(Message.Flag.DONT_BUNDLE.value());
      }
      // Only the commands in total order must be received by the originator.
      if (deliverOrder != DeliverOrder.TOTAL) {
         msg.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK.value());
      }
      if (rsvp) {
         msg.setFlag(Message.Flag.RSVP.value());
      }

      if (recipient != null) msg.setDest(recipient);
      return msg;
   }

   Buffer marshallCall(Marshaller marshaller, ReplicableCommand command) {
      Buffer buf;
      try {
         buf = marshaller.objectToBuffer(command);
      } catch (RuntimeException e) {
         throw e;
      } catch (Exception e) {
         throw new RuntimeException("Failure to marshal argument(s)", e);
      }
      return buf;
   }

   private SingleResponseFuture processSingleCall(ReplicableCommand command, long timeout,
                                                  Address destination, ResponseMode mode,
                                                  DeliverOrder deliverOrder, Marshaller marshaller) throws Exception {
      if (trace)
         log.tracef("Replication task sending %s to single recipient %s with response mode %s", command, destination, mode);
      boolean rsvp = isRsvpCommand(command);

      // Replay capability requires responses from all members!
      Buffer buf;
      buf = marshallCall(marshaller, command);
      Message msg = constructMessage(buf, destination, mode, rsvp, deliverOrder);
      NotifyingFuture<Response> request = sendMessageWithFuture(msg, new RequestOptions(mode, timeout));
      if (mode == ResponseMode.GET_NONE)
         return null;

      SingleResponseFuture retval = new SingleResponseFuture(request);
      if (timeout > 0 && !retval.isDone()) {
         ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(retval, timeout, TimeUnit.MILLISECONDS);
         retval.setTimeoutFuture(timeoutFuture);
      }
      return retval;
   }

   private void processCallsStaggered(ReplicableCommand command, RspFilter filter, List<Address> dests,
                                      ResponseMode mode, DeliverOrder deliverOrder, Marshaller marshaller,
                                      CompletableFuture<Responses> theFuture, int destIndex,
                                      long deadline, Responses rsps)
         throws Exception {
      if (destIndex == dests.size())
         return;

      CompletableFuture<Rsp<Response>> subFuture =
            processSingleCall(command, -1, dests.get(destIndex), mode, deliverOrder, marshaller);
      if (subFuture != null) {
         subFuture.whenComplete((rsp, throwable) -> {
            if (throwable != null) {
               // We should never get here, any remote exception will be in the Rsp
               theFuture.completeExceptionally(throwable);
            }
            rsps.addResponse(rsp);
            // Do not fail the request if one response is suspected/unreachable
            if (rsp.wasReceived() && (filter == null || filter.isAcceptable(rsp.getValue(), rsp.getSender()))) {
               // We got an acceptable response
               theFuture.complete(rsps);
            } else {
               // We only give up after we've received invalid responses from all recipients,
               // even after the stagger timeout expired for them.
               if (!rsps.isMissingResponses()) {
                  // This was the last response, need to complete the future
                  theFuture.complete(rsps);
               } else {
                  // The response was not acceptable, complete the timeout future to start the next request
                  staggeredProcessNext(command, filter, dests, mode, deliverOrder, marshaller, theFuture,
                        destIndex, deadline, rsps);
               }
            }
         });
         if (!subFuture.isDone()) {
            long delayNanos = timeService.remainingTime(deadline, TimeUnit.NANOSECONDS);
            if (destIndex < dests.size() - 1) {
               // Not the last recipient, wait for STAGGER_DELAY_NANOS only
               delayNanos = Math.min(STAGGER_DELAY_NANOS, delayNanos);
            }
            ScheduledFuture<?> timeoutTask = timeoutExecutor.schedule(
                  () -> staggeredProcessNext(command, filter, dests, mode, deliverOrder, marshaller,
                        theFuture, destIndex, deadline, rsps), delayNanos, TimeUnit.NANOSECONDS);
            theFuture.whenComplete((rsps1, throwable) -> timeoutTask.cancel(false));
         }
      } else {
         staggeredProcessNext(command, filter, dests, mode, deliverOrder, marshaller, theFuture, destIndex,
               deadline, rsps);
      }
   }

   private void staggeredProcessNext(ReplicableCommand command, RspFilter filter, List<Address> dests,
                                     ResponseMode mode, DeliverOrder deliverOrder, Marshaller marshaller,
                                     CompletableFuture<Responses> theFuture, int destIndex, long deadline,
                                     Responses rsps) {
      if (theFuture.isDone()) {
         return;
      }
      if (timeService.isTimeExpired(deadline)) {
         rsps.setTimedOut();
         theFuture.complete(rsps);
         return;
      }
      try {
         processCallsStaggered(command, filter, dests, mode, deliverOrder, marshaller, theFuture,
               destIndex + 1, deadline, rsps);
      } catch (Exception e) {
         // We should never get here, any remote exception will be in the Rsp
         theFuture.completeExceptionally(e);
      }
   }

   private CompletableFuture<Responses> processCalls(ReplicableCommand command, boolean broadcast, long timeout,
                                                     RspFilter filter, List<Address> dests, ResponseMode mode,
                                                     DeliverOrder deliverOrder, Marshaller marshaller) throws Exception {
      if (trace) log.tracef("Replication task sending %s to addresses %s with response mode %s", command, dests, mode);
      boolean rsvp = isRsvpCommand(command);

      Buffer buf = marshallCall(marshaller, command);
      Message msg;
      RequestOptions opts;
      if (deliverOrder == DeliverOrder.TOTAL) {
         msg = constructMessage(buf, new AnycastAddress(dests), mode, rsvp, deliverOrder);
         opts = new RequestOptions(mode, timeout, false, filter);
      } else if (broadcast || FORCE_MCAST) {
         msg = constructMessage(buf, null, mode, rsvp, deliverOrder);
         opts = new RequestOptions(mode, timeout, false, filter);
      } else {
         msg = constructMessage(buf, null, mode, rsvp, deliverOrder);
         opts = new RequestOptions(mode, timeout, true, filter);
      }

      RspListFuture retval = new RspListFuture();
      GroupRequest<Response> request = this.cast(dests, msg, opts, false, retval);
      if (mode == ResponseMode.GET_NONE)
         return null;

      if (request == null) {
         // cast() returns null when there no other nodes in the cluster
         if (broadcast) {
            retval.complete(Responses.EMPTY);
         } else {
            // TODO Use Responses.EMPTY here too
            retval.complete(Responses.suspected(dests));
         }
      }
      if (timeout > 0 && !retval.isDone()) {
         retval.setRequest(request);
         ScheduledFuture<?> timeoutFuture = timeoutExecutor.schedule(retval, timeout, TimeUnit.MILLISECONDS);
         retval.setTimeoutFuture(timeoutFuture);
      }
      return retval;
   }

   private static boolean isRsvpCommand(ReplicableCommand command) {
      return command instanceof FlagAffectedCommand
            && ((FlagAffectedCommand) command).hasFlag(Flag.GUARANTEED_DELIVERY);
   }

}
