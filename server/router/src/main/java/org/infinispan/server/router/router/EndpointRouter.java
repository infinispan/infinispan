package org.infinispan.server.router.router;

import java.net.InetAddress;
import java.util.Optional;

import org.infinispan.server.router.RoutingTable;

/**
 * The EndpointRouter interface. Currently the EndpointRouter is coupled closely with the the {@link Protocol} it implements.
 *
 * @author Sebastian Łaskawiec
 */
public interface EndpointRouter {

    /**
     * The protocol the router implements.
     */
    enum Protocol {
        HOT_ROD, SINGLE_PORT, REST
    }

    /**
     * Starts the {@link EndpointRouter}.
     *
     * @param routingTable {@link RoutingTable} for supplying {@link org.infinispan.server.router.routes.Route}s.
     */
    void start(RoutingTable routingTable);

    /**
     * Stops the {@link EndpointRouter}.
     */
    void stop();

    /**
     * Gets {@link EndpointRouter} IP address. This may return {@link Optional#empty()} if the {@link EndpointRouter} is stopped.
     */
    InetAddress getIp();

    /**
     * Gets {@link EndpointRouter} port. This may return {@link Optional#empty()} if the {@link EndpointRouter} is stopped.
     */
    Integer getPort();

    /**
     * Gets {@link Protocol} implemented by this {@link EndpointRouter}.
     */
    Protocol getProtocol();

}
