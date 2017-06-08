package org.infinispan.remoting.transport.jgroups;

import static org.infinispan.interceptors.locking.PessimisticLockingInterceptor.trace;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.Response;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

/**
 * Extend {@link RequestCorrelator} to use our own marshaller.
 *
 * @author Dan Berindei
 * @since 9.0
 */
class CustomRequestCorrelator extends RequestCorrelator {
   private static final Log log = LogFactory.getLog(CustomRequestCorrelator.class);
   private final Executor remoteExecutor;
   private final StreamingMarshaller ispnMarshaller;

   CustomRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr,
         Executor remoteExecutor, StreamingMarshaller ispnMarshaller) {
      // Make sure we use the same protocol id as the parent class
      super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, local_addr);
      this.remoteExecutor = remoteExecutor;
      this.ispnMarshaller = ispnMarshaller;
   }

   @Override
   public void receiveView(View new_view) {
      // Suspecting a node may unblock some commands, which can potentially block that thread for a long time.
      // We don't want to block view handling, so we unblock the commands on a separate thread.
      // Ideally, we'd unblock each command on a separate thread.
      // For regular responses, it's ok to block the OOB thread that received the response:
      remoteExecutor.execute(() -> super.receiveView(new_view));
   }

   @Override
   public void receiveMessageBatch(MessageBatch batch) {
      //only regular messages!
      if (batch.mode() == MessageBatch.Mode.REG) {
         batch.forEach(this::handleRegularMessage);
         return;
      }

      List<PendingMessage> orderedCommands = new LinkedList<>();
      for (Message msg : batch) {
         Header hdr = msg.getHeader(this.corr_id);
         if (hdr == null || hdr.corrId != this.corr_id) // msg was sent by a different request corr in the same stack
         {
            continue;
         }

         if (hdr instanceof MultiDestinationHeader) {
            // if we are part of the exclusion list, then we discard the request (addressed to different members)
            Address[] exclusion_list = ((MultiDestinationHeader) hdr).exclusion_list;
            if (exclusion_list != null && local_addr != null && Util.contains(local_addr, exclusion_list)) {
               batch.remove(msg);
               continue; // don't pass this message further up
            }
         }
         dispatchMessageFromBatch(msg, hdr, orderedCommands);
      }
      for (PendingMessage pendingMessage : orderedCommands) {
         Message msg = pendingMessage.message;
         Response rsp = pendingMessage.response;
         dispatcher().handleCommand(msg, rsp);
      }
   }

   @Override
   protected void dispatch(Message msg, Header hdr) {
      switch (hdr.type) {
         case Header.REQ:
            final Response rsp = hdr.rspExpected() ? new ResponseImpl(msg, hdr.req_id) : null;
            if (msg.length() == 0) {
               dispatcher().reply(rsp, null, null, msg);
            } else {
               dispatcher().handleCommand(msg, rsp);
            }
            break;
         case Header.RSP:
         case Header.EXC_RSP:
            Request req = requests.get(hdr.req_id);
            if (req != null) {
               remoteExecutor.execute(() -> handleInfinispanResponse(req, msg, hdr.type == Header.EXC_RSP));

            }
            break;
         default:
            log.error(Util.getMessage("HeaderSTypeIsNeitherREQNorRSP"));
            break;
      }
   }

   private void handleRegularMessage(Message msg, MessageBatch batch) {
      Header hdr = msg.getHeader(this.corr_id);
      if (hdr == null || hdr.corrId != this.corr_id) // msg was sent by a different request corr in the same stack
      {
         return;
      }

      if (hdr instanceof MultiDestinationHeader) {
         // if we are part of the exclusion list, then we discard the request (addressed to different members)
         Address[] exclusion_list = ((MultiDestinationHeader) hdr).exclusion_list;
         if (exclusion_list != null && local_addr != null && Util.contains(local_addr, exclusion_list)) {
            batch.remove(msg);
            return; // don't pass this message further up
         }
      }
      dispatch(msg, hdr);
   }

   private void dispatchMessageFromBatch(Message msg, Header hdr, List<PendingMessage> orderedCommands) {
      switch (hdr.type) {
         case Header.REQ:
            handleInfinispanRequest(msg, hdr, orderedCommands);
            break;
         case Header.RSP:
         case Header.EXC_RSP:
            Request req = requests.get(hdr.req_id);
            if (req != null) {
               remoteExecutor.execute(() -> handleInfinispanResponse(req, msg, hdr.type == Header.EXC_RSP));

            }
            break;
         default:
            log.error(Util.getMessage("HeaderSTypeIsNeitherREQNorRSP"));
            break;
      }
   }

   private void handleInfinispanResponse(Request req, Message message, boolean is_exception) {
      Object retval;
      if (message.length() == 0) {
         // Empty buffer signals the ForkChannel with this name is not running on the remote node
         retval = CacheNotFoundResponse.INSTANCE;
      } else {
         try {
            retval = ispnMarshaller.objectFromByteBuffer(message.rawBuffer(), message.offset(), message.length());
         } catch (Exception e) {
            log.error(Util.getMessage("FailedUnmarshallingBufferIntoReturnValue"), e);
            retval = e;
            is_exception = true;
         }
      }
      req.receiveResponse(retval, message.src(), is_exception);
   }

   private void handleInfinispanRequest(Message msg, Header hdr, List<PendingMessage> orderedCommands) {
      final Response rsp = hdr.rspExpected() ? new ResponseImpl(msg, hdr.req_id) : null;
      if (msg.length() == 0) {
         dispatcher().reply(rsp, null, null, msg);
      } else {
         switch (CommandAwareRpcDispatcher.decodeDeliverMode(msg)) {
            case NONE:
               try {
                  final ReplicableCommand cmd = unmarshall(msg);
                  if (cmd.canBlock()) {
                     dispatcher().handleCommand(cmd, msg, rsp);
                  } else {
                     remoteExecutor.execute(() -> dispatcher().handleCommand(cmd, msg, rsp));
                  }

               } catch (IllegalLifecycleStateException e) {
                  if (trace) {
                     log.trace("Ignoring command unmarshalling error during shutdown");
                  }
                  // If this wasn't a CacheRpcCommand, it means the channel is already stopped, and the response won't matter
                  dispatcher().reply(rsp, CacheNotFoundResponse.INSTANCE, null, msg);
               } catch (Throwable x) {
                  log.errorUnMarshallingCommand(x);
                  dispatcher().reply(rsp, new ExceptionResponse(new CacheException("Problems invoking command.", x)), null, msg);
               }
               dispatcher().handleCommand(msg, rsp);
               break;
            case TOTAL:
            case PER_SENDER:
               orderedCommands.add(new PendingMessage(msg, rsp));
               break;
            default:
               throw new IllegalStateException();
         }
      }
   }

   private ReplicableCommand unmarshall(Message req) throws IOException, ClassNotFoundException {
      return Objects.requireNonNull(
            (ReplicableCommand) ispnMarshaller.objectFromByteBuffer(req.rawBuffer(), req.offset(), req.length()),
            "Unable to execute a null command!  Message was " + req);
   }


   private CommandAwareRpcDispatcher dispatcher() {
      return (CommandAwareRpcDispatcher) request_handler;
   }

   private static class PendingMessage {
      private final Message message;
      private final Response response;

      private PendingMessage(Message message, Response response) {
         this.message = message;
         this.response = response;
      }
   }
}
