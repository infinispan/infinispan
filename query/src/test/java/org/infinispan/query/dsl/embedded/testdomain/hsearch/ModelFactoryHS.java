package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.ModelFactory;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ModelFactoryHS implements ModelFactory {

   public static final ModelFactory INSTANCE = new ModelFactoryHS();

   @Override
   public Account makeAccount() {
      return new AccountHS();
   }

   @Override
   public Class<AccountHS> getAccountImplClass() {
      return AccountHS.class;
   }

   @Override
   public User makeUser() {
      return new UserHS();
   }

   @Override
   public Class<UserHS> getUserImplClass() {
      return UserHS.class;
   }

   @Override
   public Transaction makeTransaction() {
      return new TransactionHS();
   }

   @Override
   public Class<TransactionHS> getTransactionImplClass() {
      return TransactionHS.class;
   }

   @Override
   public Address makeAddress() {
      return new AddressHS();
   }

   @Override
   public Class<AddressHS> getAddressImplClass() {
      return AddressHS.class;
   }
}
