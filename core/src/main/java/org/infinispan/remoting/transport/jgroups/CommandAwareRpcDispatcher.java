package org.infinispan.remoting.transport.jgroups;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
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
import org.jgroups.util.RspList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.fromJGroupsAddress;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Pedro Ruivo
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
   public static final RspList<Response> EMPTY_RESPONSES_LIST = new RspList<>();

   private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean FORCE_MCAST = Boolean.getBoolean("infinispan.unsafe.force_multicast");
   public static final int STAGGER_DELAY_MILLIS = 5;

   private final InboundInvocationHandler handler;
   private final ScheduledExecutorService timeoutExecutor;

   public CommandAwareRpcDispatcher(Channel channel, JGroupsTransport transport,
                                    InboundInvocationHandler globalHandler, ScheduledExecutorService timeoutExecutor) {
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
   public CompletableFuture<RspList<Response>> invokeRemoteCommands(List<Address> recipients, ReplicableCommand command,
                                                       ResponseMode mode, long timeout, RspFilter filter,
                                                       DeliverOrder deliverOrder) {
      CompletableFuture<RspList<Response>> future;
      try {
         if (recipients != null && mode == ResponseMode.GET_FIRST) {
            future = new CompletableFuture<>();
            // We populate the RspList ahead of time to avoid additional synchronization afterwards
            RspList<Response> rsps = new RspList<>();
            for (Address recipient : recipients) {
               rsps.put(recipient, new Rsp<>(recipient));
            }
            processCallsStaggered(command, filter, recipients, mode, deliverOrder, req_marshaller, future, 0,
                  rsps);
            if (!future.isDone()) {
               timeoutExecutor.schedule(() -> future.complete(rsps), timeout, TimeUnit.MILLISECONDS);
            }
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
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd);
      handler.handleFromRemoteSite(siteAddress.getSite(), (XSiteReplicateCommand) cmd, reply, decodeDeliverMode(req));
   }

   private void executeCommandFromLocalCluster(final ReplicableCommand cmd, final Message req, final org.jgroups.blocks.Response response) throws Throwable {
      Reply reply = returnValue -> CommandAwareRpcDispatcher.this.reply(response, returnValue, cmd);
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
         CompletableFuture<RspList<Response>> theFuture, int destIndex, RspList<Response> rsps)
         throws Exception {
      if (destIndex == dests.size())
         return;

      CompletableFuture<Void> triggerNextFuture = new CompletableFuture<>();
      CompletableFuture<Rsp<Response>> subFuture =
            processSingleCall(command, -1, dests.get(destIndex), mode, deliverOrder, marshaller);
      if (subFuture != null) {
         subFuture.whenComplete((rsp, throwable) -> {
            if (throwable != null) {
               // We should never get here, any remote exception will be in the Rsp
               theFuture.completeExceptionally(throwable);
            }
            Rsp<Response> futureRsp = rsps.get(rsp.getSender());
            if (rsp.hasException()) {
               futureRsp.setException(rsp.getException());
            } else {
               futureRsp.setValue(rsp.getValue());
            }
            if (filter.isAcceptable(rsp.getValue(), rsp.getSender())) {
               // We got an acceptable response
               theFuture.complete(rsps);
            } else {
               boolean missingResponses = false;
               for (Rsp<Response> rsp1 : rsps) {
                  if (!rsp1.wasReceived()) {
                     missingResponses = true;
                     break;
                  }
               }
               if (!missingResponses) {
                  // This was the last response, need to complete the future
                  theFuture.complete(rsps);
               } else {
                  // The response was not acceptable, complete the timeout future to start the next request
                  triggerNextFuture.complete(null);
               }
            }
         });
      }
      if (subFuture != null && !subFuture.isDone()) {
         timeoutExecutor
               .schedule(() -> triggerNextFuture.complete(null), STAGGER_DELAY_MILLIS, TimeUnit.MILLISECONDS);
      }
      triggerNextFuture.thenAccept(ignored -> {
         if (theFuture.isDone()) {
            return;
         }
         try {
            processCallsStaggered(command, filter, dests, mode, deliverOrder, marshaller, theFuture,
                  destIndex + 1, rsps);
         } catch (Exception e) {
            // We should never get here, any remote exception will be in the Rsp
            theFuture.completeExceptionally(e);
         }
      });
   }

   private RspListFuture processCalls(ReplicableCommand command, boolean broadcast, long timeout,
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

      GroupRequest<Response> request = cast(dests, msg, opts, false);
      if (mode == ResponseMode.GET_NONE)
         return null;

      RspListFuture retval = new RspListFuture(request);
      if (request == null) {
         // cast() returns null when there no other nodes in the cluster
         if (broadcast) {
            retval.complete(EMPTY_RESPONSES_LIST);
         } else {
            // TODO Use EMPTY_RESPONSES_LIST here too
            List<Rsp<Response>> rsps = new ArrayList<>(dests.size());
            for (Address dest : dests) {
               Rsp<Response> rsp = new Rsp<>(dest);
               rsp.setSuspected();
               rsps.add(rsp);
            }
            retval.complete(new RspList<>(rsps));
         }
      }
      if (timeout > 0 && !retval.isDone()) {
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
