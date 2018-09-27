package org.infinispan.query.affinity;

import java.util.Objects;

import org.infinispan.remoting.transport.Address;

/**
 * @since 9.0
 */
final class ShardAddress {

   private final String shard;
   private final Address address;

   ShardAddress(String shard, Address address) {
      this.shard = shard;
      this.address = address;
   }

   String getShard() {
      return shard;
   }

   Address getAddress() {
      return address;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || this.getClass() != o.getClass()) return false;
      ShardAddress that = (ShardAddress) o;
      return Objects.equals(shard, that.shard) &&
            Objects.equals(address, that.address);
   }

   @Override
   public int hashCode() {
      return Objects.hash(shard, address);
   }

   @Override
   public String toString() {
      return "ShardAddress{" +
            "shard='" + shard + '\'' +
            ", address=" + address +
            '}';
   }
}
