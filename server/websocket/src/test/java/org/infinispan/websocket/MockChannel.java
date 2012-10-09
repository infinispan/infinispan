/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.websocket;

import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketAddress;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.handler.codec.http.websocket.DefaultWebSocketFrame;
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
	 * @see org.jboss.netty.channel.Channel#getConfig()
	 */
	@Override
	public ChannelConfig getConfig() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getLocalAddress()
	 */
	@Override
	public SocketAddress getLocalAddress() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getRemoteAddress()
	 */
	@Override
	public SocketAddress getRemoteAddress() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#isBound()
	 */
	@Override
	public boolean isBound() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#isConnected()
	 */
	@Override
	public boolean isConnected() {
		return false;
	}



	@Override
   public Object getAttachment() {
      return null;
   }

   @Override
   public void setAttachment(Object attachment) {
   }

   /* (non-Javadoc)
	 * @see org.jboss.netty.channel.AbstractChannel#write(java.lang.Object)
	 */
	@Override
	public ChannelFuture write(Object message) {
		if(message instanceof DefaultWebSocketFrame) {
			writer.write(((DefaultWebSocketFrame)message).getTextData());
		} else {
			throw new IllegalStateException("Expected a DefaultWebSocketFrame.");
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
	 * @see org.jboss.netty.channel.Channel#bind(java.net.SocketAddress)
	 */
	@Override
	public ChannelFuture bind(SocketAddress arg0) {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#close()
	 */
	@Override
	public ChannelFuture close() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#connect(java.net.SocketAddress)
	 */
	@Override
	public ChannelFuture connect(SocketAddress arg0) {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#disconnect()
	 */
	@Override
	public ChannelFuture disconnect() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getCloseFuture()
	 */
	@Override
	public ChannelFuture getCloseFuture() {
		return (ChannelFuture) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[] { ChannelFuture.class },
                new InvocationHandler() {
					@Override
               public Object invoke(Object proxy, Method method,Object[] args) throws Throwable {
						return null;
					}
		});
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getFactory()
	 */
	@Override
	public ChannelFactory getFactory() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getId()
	 */
	@Override
	public Integer getId() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getInterestOps()
	 */
	@Override
	public int getInterestOps() {
		return 0;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getParent()
	 */
	@Override
	public Channel getParent() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#getPipeline()
	 */
	@Override
	public ChannelPipeline getPipeline() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#isReadable()
	 */
	@Override
	public boolean isReadable() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#isWritable()
	 */
	@Override
	public boolean isWritable() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#setInterestOps(int)
	 */
	@Override
	public ChannelFuture setInterestOps(int arg0) {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#setReadable(boolean)
	 */
	@Override
	public ChannelFuture setReadable(boolean arg0) {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#unbind()
	 */
	@Override
	public ChannelFuture unbind() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.Channel#write(java.lang.Object, java.net.SocketAddress)
	 */
	@Override
	public ChannelFuture write(Object arg0, SocketAddress arg1) {
		return null;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Channel o) {
		return 0;
	}
}
