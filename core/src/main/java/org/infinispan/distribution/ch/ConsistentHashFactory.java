package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

import java.util.List;
import java.util.Map;

/**
 * Factory for {@link ConsistentHash} instances.
 *
 * @see <a href="https://community.jboss.org/wiki/Non-BlockingStateTransferV2">Non-BlockingStateTransferV2</a>
 *
 * @author Dan Berindei
 * @since 5.2
 */
public interface ConsistentHashFactory<CH extends ConsistentHash> {

   /**
    * Create a new consistent hash instance.
    *
    * @param hashFunction The hash function to use on top of the keys' own {@code hashCode()} implementation.
    * @param numOwners The ideal number of owners for each key. The created consistent hash
    *                  can have more or less owners, but each key will have at least one owner.
    * @param numSegments Number of hash-space segments. The implementation may round up the number
    *                    of segments for performance, or may ignore the parameter altogether.
    * @param members A list of addresses representing the new cache members.
    * @param capacityFactors The capacity factor of each member. Determines the relative capacity of each node compared
    *                        to the others. The implementation may ignore this parameter.
    *                        If {@code null}, all the members are assumed to have a capacity factor of 1.
    */
   CH create(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
             Map<Address, Float> capacityFactors);

   /**
    * Create a new consistent hash instance, based on an existing instance, but with a new list of members.
    * <p/>
    * This method will not assign any new owners, so it will not require a state transfer.
    * The only exception is if a segment doesn't have any owners in the new members list - but there isn't
    * anyone to transfer that segment from, so that won't require a state transfer either.
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @param newMembers A list of addresses representing the new cache members.
    * @param capacityFactors The capacity factor of each member. Determines the relative capacity of each node compared
    *                        to the others. The implementation may ignore this parameter.
    *                        If {@code null}, all the members are assumed to have a capacity factor of 1.
    * @return A new {@link ConsistentHash} instance, or {@code baseCH} if the existing instance
    *         does not need any changes.
    */
   CH updateMembers(CH baseCH, List<Address> newMembers, Map<Address, Float> capacityFactors);

   /**
    * Create a new consistent hash instance, based on an existing instance, but "balanced" according to
    * the implementation's rules.
    * <p/>
    * It must be possible to switch from the "intermediary" consistent hash that includes the
    * old owners to the new consistent hash without any state transfer.
    * <p/>
    * {@code rebalance(rebalance(ch))} must be equivalent to {@code rebalance(ch)}.
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @return A new {@link ConsistentHash} instance, or {@code baseCH} if the existing instance
    *         does not need any changes.
    */
   CH rebalance(CH baseCH);

   /**
    * Creates a union of two compatible ConsistentHashes (use the same hashing function and have the same configuration
    * parameters).
    */
   CH union(CH ch1, CH ch2);
}
