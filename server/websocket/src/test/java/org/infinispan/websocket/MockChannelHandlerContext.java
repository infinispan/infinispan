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
