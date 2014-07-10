package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ModelFactoryPB implements ModelFactory {

   public static final ModelFactory INSTANCE = new ModelFactoryPB();

   @Override
   public Account makeAccount() {
      return new AccountPB();
   }

   @Override
   public Class<?> getAccountImplClass() {
      return AccountPB.class;
   }

   @Override
   public User makeUser() {
      return new UserPB();
   }

   @Override
   public Class<UserPB> getUserImplClass() {
      return UserPB.class;
   }

   @Override
   public Address makeAddress() {
      return new AddressPB();
   }

   @Override
   public Class<AddressPB> getAddressImplClass() {
      return AddressPB.class;
   }

   @Override
   public Transaction makeTransaction() {
      return new TransactionPB();
   }

   @Override
   public Class<?> getTransactionImplClass() {
      return TransactionPB.class;
   }
}
