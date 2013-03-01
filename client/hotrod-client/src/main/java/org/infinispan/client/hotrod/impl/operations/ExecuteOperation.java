package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * ExecuteOperation.
 * 
 * @author Tristan Tarrant
 * @since 7.1
 */
public class ExecuteOperation extends RetryOnFailureOperation<byte[]> {

	private final String taskName;
	private final Map<String, byte[]> marshalledParams;

	protected ExecuteOperation(Codec codec, TransportFactory transportFactory,
			byte[] cacheName, AtomicInteger topologyId, Flag[] flags, String taskName, Map<String, byte[]> marshalledParams) {
		super(codec, transportFactory, cacheName, topologyId, flags);
		this.taskName = taskName;
		this.marshalledParams = marshalledParams;
	}

	@Override
	protected Transport getTransport(int retryCount,
			Set<SocketAddress> failedServers) {
		return transportFactory.getTransport(failedServers, cacheName);
	}

	@Override
	protected byte[] executeOperation(Transport transport) {
		HeaderParams params = writeHeader(transport, EXEC_REQUEST);
		transport.writeString(taskName);
		transport.writeVInt(marshalledParams.size());
		for(Entry<String, byte[]> entry : marshalledParams.entrySet()) {
			transport.writeString(entry.getKey());
			transport.writeArray(entry.getValue());
		}
		transport.flush();
		readHeaderAndValidate(transport, params);
		return transport.readArray();
	}

}
