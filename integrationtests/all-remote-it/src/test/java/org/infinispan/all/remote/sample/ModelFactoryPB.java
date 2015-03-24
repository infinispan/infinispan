package org.infinispan.all.remote.sample;

import org.infinispan.all.remote.sample.testdomain.Account;
import org.infinispan.all.remote.sample.testdomain.Address;
import org.infinispan.all.remote.sample.testdomain.ModelFactory;
import org.infinispan.all.remote.sample.testdomain.Transaction;
import org.infinispan.all.remote.sample.testdomain.User;

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
