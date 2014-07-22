package org.infinispan.server.test.client.hotrod;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

public class HotRodTestInterceptingTransportFactory extends TcpTransportFactory {
    private static final Log log = LogFactory.getLog(HotRodTestInterceptingTransportFactory.class);

    private InetSocketAddress sock_addr = null;

    /*
     * Returns the last server address used by this transport
     */
    public InetSocketAddress getLastServerAddress() {
        return sock_addr;
    }

    /*
     * Version of getTransport() which keeps track of InetSocketAddress values
     */
    @Override
    public Transport getTransport(Set<SocketAddress> failedServers, byte[] cacheName) {
        Transport transport = super.getTransport(failedServers, cacheName);
        log.trace("InterceptingTransport called");
        sock_addr = (InetSocketAddress) ((TcpTransport) transport).getServerAddress();
        return transport;
    }

}