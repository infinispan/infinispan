package org.infinispan.query.core.tests;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * A mutable, protobuf-annotated test entity. Its {@link WalletSchema} does not
 * override the schema package, so the protobuf message name equals the Java
 * class name: a requirement for updates on an object-storage cache to resolve
 * to the {@code PROTOBUF_ROUNDTRIP} strategy, where the entity name used by the
 * query must be both a resolvable Java class and a marshallable protobuf type.
 */
class Wallet {

   private String owner;
   private long balance;

   @ProtoField(1)
   public String getOwner() {
      return owner;
   }

   public void setOwner(String owner) {
      this.owner = owner;
   }

   @ProtoField(number = 2, defaultValue = "0")
   public long getBalance() {
      return balance;
   }

   public void setBalance(long balance) {
      this.balance = balance;
   }
}
