package org.infinispan.remoting.jgroups;


import org.jgroups.Message;
import org.jgroups.protocols.SimplifiedTransferQueueBundler;

/**
 * This bundler extends {@link SimplifiedTransferQueueBundler} to drop messages if the queue is full.
 *
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available.
 *
 * When the queue is full, messages are discarded immediately.
 *
 * @see org.jgroups.protocols.TransferQueueBundler
 */
public class NonBlockingTransferQueueBundler extends SimplifiedTransferQueueBundler {
    @Override
    public void send(Message msg) {
        if(this.running)
            this.queue.offer(msg);
    }
}
