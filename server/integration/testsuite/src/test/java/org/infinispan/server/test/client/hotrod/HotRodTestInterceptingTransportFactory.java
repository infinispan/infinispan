package org.infinispan.server.test.client.hotrod;

import java.net.InetSocketAddress;

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
    public Transport getTransport() {
        Transport transport = super.getTransport();
        System.out.println("InterceptingTransport called");
        sock_addr = (InetSocketAddress) ((TcpTransport) transport).getServerAddress();
        return transport;
    }

}