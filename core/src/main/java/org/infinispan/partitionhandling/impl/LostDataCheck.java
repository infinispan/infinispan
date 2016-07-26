package org.infinispan.partitionhandling.impl;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.function.BiPredicate;

public interface LostDataCheck extends BiPredicate<ConsistentHash, List<Address>> {
}
