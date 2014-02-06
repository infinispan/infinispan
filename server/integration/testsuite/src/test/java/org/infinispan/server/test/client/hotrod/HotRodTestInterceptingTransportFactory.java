package org.infinispan.server.test.client.hotrod;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;

public class HotRodTestInterceptingTransportFactory extends TcpTransportFactory {

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
    public Transport getTransport(Set<SocketAddress> failedServers) {
        Transport transport = super.getTransport(failedServers);
        System.out.println("InterceptingTransport called");
        sock_addr = (InetSocketAddress) ((TcpTransport) transport).getServerAddress();
        return transport;
    }

}