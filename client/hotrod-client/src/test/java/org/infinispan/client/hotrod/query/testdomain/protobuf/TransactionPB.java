package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.query.dsl.embedded.testdomain.Transaction;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class TransactionPB implements Transaction {

   private int id;
   private String description;
   private int accountId;
   private Date date;
   private BigDecimal amount;
   private boolean isDebit;

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }

   public int getAccountId() {
      return accountId;
   }

   public void setAccountId(int accountId) {
      this.accountId = accountId;
   }

   public Date getDate() {
      return date;
   }

   public void setDate(Date date) {
      this.date = date;
   }

   public double getAmount() {
      return amount.doubleValue();
   }

   public void setAmount(double amount) {
      this.amount = new BigDecimal(amount);
   }

   public boolean isDebit() {
      return isDebit;
   }

   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @Override
   public String toString() {
      return "TransactionPB{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", accountId=" + accountId +
            ", date=" + date +
            ", amount=" + amount +
            ", isDebit=" + isDebit +
            '}';
   }
}

