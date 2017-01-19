package org.infinispan.server.router.router;

import java.net.InetAddress;
import java.util.Optional;

import org.infinispan.server.router.RoutingTable;

/**
 * The Router interface. Currently the Router is coupled closely with the the {@link Protocol} it implements.
 *
 * @author Sebastian Łaskawiec
 */
public interface Router {

    /**
     * The protocol the router implements.
     */
    enum Protocol {
        HOT_ROD, REST
    }

    /**
     * Starts the {@link Router}.
     *
     * @param routingTable {@link RoutingTable} for supplying {@link org.infinispan.server.router.routes.Route}s.
     */
    void start(RoutingTable routingTable);

    /**
     * Stops the {@link Router}.
     */
    void stop();

    /**
     * Gets {@link Router} IP address. This may return {@link Optional#empty()} if the {@link Router} is stopped.
     */
    Optional<InetAddress> getIp();

    /**
     * Gets {@link Router} port. This may return {@link Optional#empty()} if the {@link Router} is stopped.
     */
    Optional<Integer> getPort();

    /**
     * Gets {@link Protocol} implemented by this {@link Router}.
     */
    Protocol getProtocol();

}
