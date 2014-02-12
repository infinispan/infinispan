package org.infinispan.websocket;

import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketAddress;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 *
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class MockChannel implements Channel {

	private StringWriter writer = new StringWriter();

   /* (non-Javadoc)
	 * @see org.jboss.netty.channel.AbstractChannel#write(java.lang.Object)
	 */
	@Override
	public ChannelFuture write(Object message) {
		if(message instanceof TextWebSocketFrame) {
			writer.write(((TextWebSocketFrame)message).text());
		} else {
			throw new IllegalStateException("Expected a TextWebSocketFrame but got " + message);
		}
		return null;
	}

	public JSONObject getJSONPayload() {
		if(writer.getBuffer().length() == 0) {
			return null;
		}
		return getJSONPayload(0);
	}

	public JSONObject getJSONPayload(long waitTimeout) {
		long start = System.currentTimeMillis();
		while(writer.getBuffer().length() == 0) {
			if(System.currentTimeMillis() > start + waitTimeout) {
				throw new RuntimeException("Timed out waiting for data to be pushed onto the channel.");
			}
		}

		try {
			return new JSONObject(writer.toString());
		} catch (JSONException e) {
			throw new RuntimeException("Invalid JSON payload [" + writer.toString() + "].", e);
		} finally {
			clear();
		}
	}

	public void clear() {
		writer.getBuffer().setLength(0);
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getCloseFuture()
	 */
	@Override
	public ChannelFuture closeFuture() {
		return (ChannelFuture) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[] { ChannelFuture.class },
                new InvocationHandler() {
					@Override
               public Object invoke(Object proxy, Method method,Object[] args) throws Throwable {
						return null;
					}
		});
	}

    @Override
    public EventLoop eventLoop() {
        return null;
    }

    @Override
    public Channel parent() {
        return null;
    }

    @Override
    public ChannelConfig config() {
        return null;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isRegistered() {
        return false;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public ChannelMetadata metadata() {
        return null;
    }

    @Override
    public SocketAddress localAddress() {
        return null;
    }

    @Override
    public SocketAddress remoteAddress() {
        return null;
    }

    @Override
    public boolean isWritable() {
        return false;
    }

    @Override
    public Unsafe unsafe() {
        return null;
    }

    @Override
    public ChannelPipeline pipeline() {
        return null;
    }

    @Override
    public ByteBufAllocator alloc() {
        return null;
    }

    @Override
    public ChannelPromise newPromise() {
        return null;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return null;
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return null;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return null;
    }

    @Override
    public ChannelPromise voidPromise() {
        return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return null;
    }

    @Override
    public ChannelFuture disconnect() {
        return null;
    }

    @Override
    public ChannelFuture close() {
        return null;
    }

    @Override
    public ChannelFuture deregister() {
        return null;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return null;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return null;
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
        return null;
    }

    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return null;
    }

    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        return null;
    }

    @Override
    public Channel read() {
        return null;
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        write(msg);
        return promise.setSuccess();
    }

    @Override
    public Channel flush() {
        return null;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        write(msg);
        return promise.setSuccess();
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        write(msg);
        return null;
    }

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return null;
    }

    @Override
    public int compareTo(Channel o) {
        return 0;
    }
}
