package org.infinispan.query.dsl.sample_domain_model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@Indexed
public class Transaction {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String description;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int accountId;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Date date;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private BigDecimal amount;

   @Field(store = Store.YES, analyze = Analyze.NO)
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

   public BigDecimal getAmount() {
      return amount;
   }

   public void setAmount(double amount) {
      this.amount = new BigDecimal(amount);
   }

   public void setAmount(BigDecimal amount) {
      this.amount = amount;
   }

   public boolean isDebit() {
      return isDebit;
   }

   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @Override
   public String toString() {
      return "Transaction{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", accountId=" + accountId +
            ", date=" + date +
            ", amount=" + amount +
            ", isDebit=" + isDebit +
            '}';
   }
}
