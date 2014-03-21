/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.infinispan.server.core.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import java.net.SocketAddress;
import java.util.List;

/**
 * Abstract base class for support SASL (server-side). Implementations need to extend this and provide implementations
 * for {@link #newContinueMessage(ByteBuf)}, {@link #newErrorMessage(SaslException)} and
 * {@link #newSuccessMessage(ByteBuf)}.
 *
 * As the {@link CallbackHandler} used for construct the {@link SaslServer} may block you may need to specify a
 * dedicated {@link io.netty.channel.EventLoopGroup} when adding this {@link SaslServerHandler} to
 * the {@link ChannelPipeline}. If you are sure your {@link CallbackHandler} does not block at all you not need this,
 * so it depends on the implementation itself.
 *
 * The {@link io.netty.handler.sasl.SaslServerHandler} will remove itself from the {@link ChannelPipeline} once
 * it is not needed anymore.
 *
 */
public abstract class SaslServerHandler<M> extends ChannelInboundHandlerAdapter {

    private static final String AUTH_INT = "auth-int";
    private static final String AUTO_CONF = "auth-conf";

    private SaslServer server;
    private boolean firstPass;

    public SaslServerHandler(SaslServer server) {
        this.server = server;
        this.firstPass = true;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        Channel ch = ctx.channel();
        try {
            if (!firstPass) {
               readHeader(buf);
            } else {
               firstPass = false;
            }
            byte[] bytes = readBytes(buf);
            byte[] challenge = server.evaluateResponse(bytes);
            if (!server.isComplete()) {
                ch.writeAndFlush(newContinueMessage(ctx, Unpooled.wrappedBuffer(challenge)));
            } else {
                ch.writeAndFlush(newSuccessMessage(ctx, Unpooled.wrappedBuffer(challenge)));

                ChannelPipeline pipeline = ctx.pipeline();
                String qop = (String) server.getNegotiatedProperty(Sasl.QOP);
                if (qop != null
                        && (qop.equalsIgnoreCase(AUTH_INT)
                        || qop.equalsIgnoreCase(AUTO_CONF))) {
                    SaslServer server = this.server;
                    this.server = null;
                    // Replace this handler now with the QopHandler
                    // This is mainly done as the QopHandler itself will not block at all and so we can
                    // get rid of the usage of the EventExecutorGroup after the negation took place.
                    pipeline.replace(this, ctx.name(), new QopHandler(server));
                } else {
                    // there is no need for any QOP handling so we are done now and can just remove ourself from the
                    // pipeline
                    pipeline.remove(this);
                }
            }
        } catch (SaslException e) {
            Object errorMsg = newErrorMessage(ctx, e);
            if (errorMsg != null) {
                ch.writeAndFlush(errorMsg).addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        if (server != null) {
            server.dispose();
        }
    }

    protected abstract void readHeader(ByteBuf buf);

    /**
     * Creates a new message which signals the remote peer the success of negotiation.
     *
     * @param challenge     the {@link ByteBuf} that holds the challenge data.
     * @return successMsg   the message
     */
    protected abstract M newSuccessMessage(ChannelHandlerContext ctx, ByteBuf challenge);

    /**
     * Creates a new message which signals the remote peer that the negotiation needs more data to process.
     *
     * @param challenge     the {@link ByteBuf} that holds the challenge data.
     * @return continueMsg   the message
     */
    protected abstract M newContinueMessage(ChannelHandlerContext ctx, ByteBuf challenge);

    /**
     * Creates a new message which signals the remote peer that the negation failed.
     *
     * @param e             the {@link SaslException} tat caused the error.
     * @return continueMsg  the message
     */
    protected abstract M newErrorMessage(ChannelHandlerContext ctx, SaslException e);

    /**
     * Read all readable bytes of the {@link ByteBuf} into a byte array and release the buffer.
     */
    private static byte[] readBytes(ByteBuf buffer) {
        byte[] bytes = ExtendedByteBuf.readRangedBytes(buffer);
        buffer.release();
        return bytes;
    }

    /**
     * Handles QOP of the SASL protocol.
     */
    private static final class QopHandler extends ByteToMessageDecoder implements ChannelOutboundHandler {
        private final SaslServer server;
        private final int maxBufferSize;
        private final int maxSendBufferSize;
        private int packetLength = -1;

        QopHandler(SaslServer server) {
            this.server = server;
            String maxBuf = (String) server.getNegotiatedProperty(Sasl.MAX_BUFFER);
            if (maxBuf != null) {
                maxBufferSize = Integer.parseInt(maxBuf);
            } else {
                maxBufferSize = -1;
            }
            String maxSendBuf = (String) server.getNegotiatedProperty(Sasl.RAW_SEND_SIZE);
            if (maxSendBuf != null) {
                maxSendBufferSize = Integer.parseInt(maxSendBuf);
            } else {
                maxSendBufferSize = -1;
            }
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            ByteBuf buffer = (ByteBuf) msg;
            byte[] bytes;
            int offset;
            int len;
            if (buffer.hasArray()) {
                bytes = buffer.array();
                offset = buffer.arrayOffset() + buffer.readerIndex();
                len = buffer.readableBytes();
            } else {
                bytes = readBytes(buffer);
                offset = 0;
                len = bytes.length;
            }
            byte[] wrapped = server.wrap(bytes, offset, len);
            ctx.write(ctx.alloc().buffer(4).writeInt(len));
            if (maxSendBufferSize != -1 && wrapped.length > maxSendBufferSize) {
                // The produces data is bigger then the maxSendBufferSize so split it and flush every of them directly.
                int size = wrapped.length;
                int off = 0;
                for (;;) {
                    if (size < maxSendBufferSize) {
                        ctx.writeAndFlush(Unpooled.wrappedBuffer(wrapped, off, size), promise);
                        return;
                    } else {
                        ctx.writeAndFlush(Unpooled.wrappedBuffer(wrapped, off, maxSendBufferSize));
                        off += maxSendBufferSize;
                        size -= maxSendBufferSize;
                    }
                }
            } else {
                ctx.write(Unpooled.wrappedBuffer(wrapped), promise);
            }
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            int len = packetLength;

            if (len == -1) {
                if (in.readableBytes() < 4) {
                    return;
                }
                len = packetLength = (int) in.readUnsignedInt();
                if (maxBufferSize != -1 && maxBufferSize < packetLength) {
                    TooLongFrameException ex = new TooLongFrameException(
                            "Frame exceed exceed max buffer size: " + packetLength + " > " + maxBufferSize);
                    ctx.fireExceptionCaught(ex);
                    ctx.close();
                    return;
                }
            }
            if (len > in.readableBytes()) {
                return;
            }
            // reset packet length
            packetLength = -1;
            int offset;
            byte[] array;
            if  (in.hasArray()) {
                offset = in.readerIndex() + in.arrayOffset();
                array = in.array();
                in.skipBytes(len);
            } else {
                offset = 0;
                array = new byte[len + 4];
                in.readBytes(array);
            }
            out.add(Unpooled.wrappedBuffer(server.unwrap(array, offset, len)));
        }

        @Override
        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise)
                throws Exception {
            ctx.bind(localAddress, promise);
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) throws Exception {
            ctx.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.disconnect(promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.close(promise);
        }

        @Override
        public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            ctx.deregister(promise);
        }

        @Override
        public void read(ChannelHandlerContext ctx) throws Exception {
            ctx.read();
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
            super.handlerRemoved0(ctx);
            server.dispose();
        }
    }
}
