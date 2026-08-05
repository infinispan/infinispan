package org.infinispan.distribution.ch.impl;

import java.util.List;

import org.infinispan.remoting.transport.Address;

/** Per-position topology constraint applied during rendezvous owner assignment. */
@FunctionalInterface
interface TopologyFilter {
   boolean canOwn(int ownerPosition, Address candidate, List<Address> currentOwners);
}
