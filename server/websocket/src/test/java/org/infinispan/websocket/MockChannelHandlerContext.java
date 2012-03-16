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

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;

/**
 * 
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class MockChannelHandlerContext implements ChannelHandlerContext {
	
	private MockChannel channel;

	/**
	 * @param channel
	 */
	public MockChannelHandlerContext(MockChannel channel) {
		this.channel = channel;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#canHandleDownstream()
	 */
	@Override
	public boolean canHandleDownstream() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#canHandleUpstream()
	 */
	@Override
	public boolean canHandleUpstream() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getAttachment()
	 */
	@Override
	public Object getAttachment() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getChannel()
	 */
	@Override
	public Channel getChannel() {
		return channel;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getHandler()
	 */
	@Override
	public ChannelHandler getHandler() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getName()
	 */
	@Override
	public String getName() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#getPipeline()
	 */
	@Override
	public ChannelPipeline getPipeline() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#sendDownstream(org.jboss.netty.channel.ChannelEvent)
	 */
	@Override
	public void sendDownstream(ChannelEvent arg0) {
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#sendUpstream(org.jboss.netty.channel.ChannelEvent)
	 */
	@Override
	public void sendUpstream(ChannelEvent arg0) {
	}

	/* (non-Javadoc)
	 * @see org.jboss.netty.channel.ChannelHandlerContext#setAttachment(java.lang.Object)
	 */
	@Override
	public void setAttachment(Object arg0) {
	}
}
