/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.jboss.netty.handler.codec.replay;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A fork version of {@link ReplayingDecoder} whose cumulation buffer can be
 * cleared when the request is completed. This helps keep memory consumption
 * low particularly when big objects are stored in Infinispan.
 *
 * To be more precise, the differences between the {@link ReplayingDecoder}
 * version in Netty 3.2.4 (code <a href="https://github.com/netty/netty/blob/netty-3.2.4.Final/src/main/java/org/jboss/netty/handler/codec/replay/ReplayingDecoder.java">here</a>)
 * and this one are:
 *
 * <ul>
 *    <li>Adding a new instance variable for the maximum buffer capacity</li>
 *    <li>Removing unused constructors by Infinispan servers and add the
 *    maximum capacity as constructor parameter</li>
 *    <li>Addition of the {@link org.jboss.netty.handler.codec.replay.CustomReplayingDecoder#slimDownBuffer()}
 *    method that slims down the buffers if they go above the maximum capacity</li>
 * </ul>
 *
 * This replaying decoder should be removed once Netty has been upgraded to
 * version 4.0, when buffer pooling will avoid unlimited buffer growth.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class CustomReplayingDecoder<T extends Enum<T>>
      extends SimpleChannelUpstreamHandler {

    private static Constructor unsafeDynamicBufferCtor;
    private static Constructor replayingDecoderBufferCtor;
    private static Method replayingDecoderBufferTerminate;
    private static Class replayErrorClass;

    private final AtomicReference<ChannelBuffer> cumulation =
        new AtomicReference<ChannelBuffer>();
    private final boolean unfold;
    private ChannelBuffer replayable;
    private T state;
    private int checkpoint;
    private final int maxCapacity;

    static {
       try {
          unsafeDynamicBufferCtor =
                getConstructor("org.jboss.netty.handler.codec.replay.UnsafeDynamicChannelBuffer");
          Class cl = Class.forName("org.jboss.netty.handler.codec.replay.ReplayingDecoderBuffer");
          replayingDecoderBufferCtor = getConstructor(cl);
          replayingDecoderBufferTerminate = getMethod(cl, "terminate");
          replayErrorClass = Class.forName("org.jboss.netty.handler.codec.replay.ReplayError");
       } catch (ClassNotFoundException e) {
          throw new IllegalStateException(
                "Unable to find a Netty class", e);
       }
    }

    protected CustomReplayingDecoder(T initialState, boolean unfold, int maxCapacity) {
        this.state = initialState;
        this.unfold = unfold;
        this.maxCapacity = maxCapacity;
    }

    /**
     * Stores the internal cumulative buffer's reader position.
     */
    protected void checkpoint() {
        ChannelBuffer cumulation = this.cumulation.get();
        if (cumulation != null) {
            checkpoint = cumulation.readerIndex();
        } else {
            checkpoint = -1; // buffer not available (already cleaned up)
        }
    }

    /**
     * Stores the internal cumulative buffer's reader position and updates
     * the current decoder state.
     */
    protected void checkpoint(T state) {
        checkpoint();
        setState(state);
    }

    /**
     * Returns the current state of this decoder.
     * @return the current state of this decoder
     */
    protected T getState() {
        return state;
    }

    /**
     * Sets the current state of this decoder.
     * @return the old state of this decoder
     */
    protected T setState(T newState) {
        T oldState = state;
        state = newState;
        return oldState;
    }

    /**
     * Returns the actual number of readable bytes in the internal cumulative
     * buffer of this decoder.  You usually do not need to rely on this value
     * to write a decoder.  Use it only when you muse use it at your own risk.
     * This method is a shortcut to {@link #internalBuffer() internalBuffer().readableBytes()}.
     */
    protected int actualReadableBytes() {
        return internalBuffer().readableBytes();
    }

    /**
     * Returns the internal cumulative buffer of this decoder.  You usually
     * do not need to access the internal buffer directly to write a decoder.
     * Use it only when you must use it at your own risk.
     */
    protected ChannelBuffer internalBuffer() {
        ChannelBuffer buf = cumulation.get();
        if (buf == null) {
            return ChannelBuffers.EMPTY_BUFFER;
        }
        return buf;
    }

    /**
     * Decodes the received packets so far into a frame.
     *
     * @param ctx      the context of this handler
     * @param channel  the current channel
     * @param buffer   the cumulative buffer of received packets so far.
     *                 Note that the buffer might be empty, which means you
     *                 should not make an assumption that the buffer contains
     *                 at least one byte in your decoder implementation.
     * @param state    the current decoder state ({@code null} if unused)
     *
     * @return the decoded frame
     */
    protected abstract Object decode(ChannelHandlerContext ctx,
            Channel channel, ChannelBuffer buffer, T state) throws Exception;

    /**
     * Decodes the received data so far into a frame when the channel is
     * disconnected.
     *
     * @param ctx      the context of this handler
     * @param channel  the current channel
     * @param buffer   the cumulative buffer of received packets so far.
     *                 Note that the buffer might be empty, which means you
     *                 should not make an assumption that the buffer contains
     *                 at least one byte in your decoder implementation.
     * @param state    the current decoder state ({@code null} if unused)
     *
     * @return the decoded frame
     */
    protected Object decodeLast(
            ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, T state) throws Exception {
        return decode(ctx, channel, buffer, state);
    }

    /**
     * Slim down internal buffer if it exceeds the limit established on creation
     */
    protected void slimDownBuffer() {
       ChannelBuffer buf = cumulation.get();
       if (buf != null && buf.capacity() > maxCapacity) {
          if (cumulation.compareAndSet(buf, null))
             replayable = null;
       }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {

        Object m = e.getMessage();
        if (!(m instanceof ChannelBuffer)) {
            ctx.sendUpstream(e);
            return;
        }

        ChannelBuffer input = (ChannelBuffer) m;
        if (!input.readable()) {
            return;
        }

        ChannelBuffer cumulation = cumulation(ctx);
        cumulation.discardReadBytes();
        cumulation.writeBytes(input);
        callDecode(ctx, e.getChannel(), cumulation, e.getRemoteAddress());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx,
            ChannelStateEvent e) throws Exception {
        cleanup(ctx, e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx,
            ChannelStateEvent e) throws Exception {
        cleanup(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        ctx.sendUpstream(e);
    }

    private void callDecode(ChannelHandlerContext context, Channel channel, ChannelBuffer cumulation, SocketAddress remoteAddress) throws Exception {
        while (cumulation.readable()) {
            int oldReaderIndex = checkpoint = cumulation.readerIndex();
            Object result = null;
            T oldState = state;
            try {
                result = decode(context, channel, replayable, state);
                if (result == null) {
                    if (oldReaderIndex == cumulation.readerIndex() && oldState == state) {
                        throw new IllegalStateException(
                                "null cannot be returned if no data is consumed and state didn't change.");
                    } else {
                        // Previous data has been discarded or caused state transition.
                        // Probably it is reading on.
                        continue;
                    }
                }
            } catch (Error replay) {
                if (replayErrorClass.isInstance(replay)) {
                    // Return to the checkpoint (or oldPosition) and retry.
                    int checkpoint = this.checkpoint;
                    if (checkpoint >= 0) {
                        cumulation.readerIndex(checkpoint);
                    } else {
                        // Called by cleanup() - no need to maintain the readerIndex
                        // anymore because the buffer has been released already.
                    }
                } else {
                   throw replay;
                }
            }

            if (result == null) {
                // Seems like more data is required.
                // Let us wait for the next notification.
                break;
            }

            if (oldReaderIndex == cumulation.readerIndex() && oldState == state) {
                throw new IllegalStateException(
                        "decode() method must consume at least one byte " +
                        "if it returned a decoded message (caused by: " +
                        getClass() + ")");
            }

            // A successful decode
            unfoldAndfireMessageReceived(context, result, remoteAddress);
        }
    }

    private void unfoldAndfireMessageReceived(
            ChannelHandlerContext context, Object result, SocketAddress remoteAddress) {
        if (unfold) {
            if (result instanceof Object[]) {
                for (Object r: (Object[]) result) {
                    Channels.fireMessageReceived(context, r, remoteAddress);
                }
            } else if (result instanceof Iterable<?>) {
                for (Object r: (Iterable<?>) result) {
                    Channels.fireMessageReceived(context, r, remoteAddress);
                }
            } else {
                Channels.fireMessageReceived(context, result, remoteAddress);
            }
        } else {
            Channels.fireMessageReceived(context, result, remoteAddress);
        }
    }

    private void cleanup(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        try {
            ChannelBuffer cumulation = this.cumulation.getAndSet(null);
            if (cumulation == null) {
                return;
            }

            replayingDecoderBufferTerminate.invoke(replayable, null);

            if (cumulation.readable()) {
                // Make sure all data was read before notifying a closed channel.
                callDecode(ctx, e.getChannel(), cumulation, null);
            }

            // Call decodeLast() finally.  Please note that decodeLast() is
            // called even if there's nothing more to read from the buffer to
            // notify a user that the connection was closed explicitly.
            Object partiallyDecoded = decodeLast(ctx, e.getChannel(), replayable, state);
            if (partiallyDecoded != null) {
                unfoldAndfireMessageReceived(ctx, partiallyDecoded, null);
            }
        } catch (ReplayError replay) {
            // Ignore
        } finally {
            ctx.sendUpstream(e);
        }
    }

    private ChannelBuffer cumulation(ChannelHandlerContext ctx) {
        ChannelBuffer buf = cumulation.get();
        if (buf == null) {
            ChannelBufferFactory factory = ctx.getChannel().getConfig().getBufferFactory();
            buf = createUnsafeDynamicChannelBuffer(factory);
            if (cumulation.compareAndSet(null, buf)) {
                replayable = createReplayingDecoderBuffer(buf);
            } else {
                buf = cumulation.get();
            }
        }
        return buf;
    }

    private ChannelBuffer createReplayingDecoderBuffer(ChannelBuffer buf) {
       try {
          return (ChannelBuffer) replayingDecoderBufferCtor.newInstance(new Object[]{buf});
       } catch (Exception e) {
          throw new IllegalStateException(
                "Unable to instantiate Netty's replaying decoder buffer", e);
       }
    }

    private ChannelBuffer createUnsafeDynamicChannelBuffer(ChannelBufferFactory factory) {
       try {
          return (ChannelBuffer) unsafeDynamicBufferCtor.newInstance(new Object[]{factory});
       } catch (Exception e) {
          throw new IllegalStateException(
                "Unable to instantiate Netty's unsafe dynamic channel buffer", e);
       }
    }

    private static Constructor getConstructor(String className) throws ClassNotFoundException {
       return getConstructor(Class.forName(className));
    }

    private static Constructor getConstructor(Class cl) throws ClassNotFoundException {
       Constructor ctor = cl.getDeclaredConstructors()[0];
       ctor.setAccessible(true);
       return ctor;
    }

    private static Method getMethod(Class cl, String methodName) {
       Method[] methods = cl.getDeclaredMethods();
       for (Method method : methods) {
          if (method.getName().equals(methodName)) {
             method.setAccessible(true);
             return method;
          }
       }
       return null;
    }

}
