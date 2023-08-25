package org.infinispan.server.resp.logging;

import java.time.temporal.Temporal;
import java.util.List;

import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.RespCommand;

import io.netty.channel.ChannelHandlerContext;

/**
 * Tracker for a single command.
 * <p>
 * One instance tracks one request at a time. One instance can be reutilized indefinitely for subsequent requests within
 * the same {@link ChannelHandlerContext} but never for interleaving requests.
 * <p>
 * A request starts with a call to {@link #track(RespCommand, List)}, storing data about time, request size, and
 * affected keys. The tracker is updated when the {@link org.infinispan.server.resp.ByteBufPool} needs to allocate more
 * bytes for a response. The request stops tracking on the {@link #done(Throwable)}, generating an instance of
 * {@link AccessData}.
 * <p>
 *
 */
public class Tracker {
   private final TimeService timeService;
   private final ChannelHandlerContext ctx;
   private byte[][] keys;
   private Temporal start;
   private int bytesRequested;
   private int requestSize;
   private RespCommand req;

   public Tracker(ChannelHandlerContext ctx, TimeService timeService) {
      this.timeService = timeService;
      this.ctx = ctx;
   }

   /**
    * Starts tracking the request.
    * <p>
    * Only one request is tracked at a time. If this method is called while another request is being tracked, an
    * {@link IllegalStateException} is thrown.
    *
    * @param req: The current request to track.
    * @param arguments: The request arguments.
    * @throws IllegalStateException if another request is being tracked.
    */
   public void track(RespCommand req, List<byte[]> arguments) {
      if (start != null) throw new IllegalStateException("Interleaving command not allowed!");

      start = timeService.instant();
      bytesRequested = 0;
      keys = req.extractKeys(arguments);
      requestSize = req.size(arguments);
      this.req = req;
   }

   public void increaseBytesRequested(int bytes) {
      bytesRequested += bytes;
   }

   /**
    * Stops tracking the request.
    * <p>
    * If this method is called while no request is being tracked, {@code null} is returned.
    *
    * @param throwable: If the request failed, the exception that caused the failure.
    * @return An instance of {@link AccessData} if a request was being tracked, {@code null} otherwise.
    */
   public AccessData done(Throwable throwable) {
      if (start == null) return null;

      AccessData ad = AccessData.create(ctx, req, start, keys, requestSize, bytesRequested, throwable);
      start = null;
      return ad;
   }
}
