package org.infinispan.distribution.ch;

import java.util.List;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

public class TopologyAwareConsistentHash extends DefaultConsistentHash {

    public TopologyAwareConsistentHash(Hash hashFunction, int numSegments, int numOwners, List<Address> members, List<Address>[] segmentOwners) {
        super(hashFunction, numSegments, numOwners, members, segmentOwners);
    }

}
