package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.marshall.StreamingMarshaller;
import java.util.concurrent.Executor;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

/**
 * Extend {@link RequestCorrelator} to use our own marshaller.
 *
 * @author Dan Berindei
 * @since 9.0
 */
class CustomRequestCorrelator extends RequestCorrelator {
   private final Executor remoteExecutor;
   private final StreamingMarshaller ispnMarshaller;

   public CustomRequestCorrelator(Protocol transport, RequestHandler handler, Address local_addr,
                                  Executor remoteExecutor, StreamingMarshaller ispnMarshaller) {
      // Make sure we use the same protocol id as the parent class
      super(ClassConfigurator.getProtocolId(RequestCorrelator.class), transport, handler, local_addr);
      this.remoteExecutor = remoteExecutor;
      this.ispnMarshaller = ispnMarshaller;
   }

   @Override
   protected void handleResponse(Request req, Address sender, byte[] buf, int offset, int length,
                                 boolean is_exception) {
      Object retval;
      if (length == 0) {
         // Empty buffer signals the ForkChannel with this name is not running on the remote node
         retval = CacheNotFoundResponse.INSTANCE;
      } else {
         try {
            retval = ispnMarshaller.objectFromByteBuffer(buf, offset, length);
         } catch (Exception e) {
            log.error(Util.getMessage("FailedUnmarshallingBufferIntoReturnValue"), e);
            retval = e;
            is_exception = true;
         }
      }
      req.receiveResponse(retval, sender, is_exception);
   }

   @Override
   public void receiveView(View new_view) {
      // Suspecting a node may unblock some commands, which can potentially block that thread for a long time.
      // We don't want to block view handling, so we unblock the commands on a separate thread.
      // Ideally, we'd unblock each command on a separate thread.
      // For regular responses, it's ok to block the OOB thread that received the response:
      remoteExecutor.execute(() -> super.receiveView(new_view));
   }
}
