package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.NumericField;
import org.hibernate.search.annotations.Store;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;

import java.io.Serializable;
import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
public class TransactionHS implements Transaction, Serializable {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String description;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int accountId;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Date date;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @NumericField
   private double amount;

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

   public double getAmount() {
      return amount;
   }

   public void setAmount(double amount) {
      this.amount = amount;
   }

   public boolean isDebit() {
      return isDebit;
   }

   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransactionHS other = (TransactionHS) o;

      if (accountId != other.accountId) return false;
      if (Double.compare(other.amount, amount) != 0) return false;
      if (id != other.id) return false;
      if (isDebit != other.isDebit) return false;
      if (date != null ? !date.equals(other.date) : other.date != null) return false;
      if (description != null ? !description.equals(other.description) : other.description != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      long temp;
      result = id;
      result = 31 * result + (description != null ? description.hashCode() : 0);
      result = 31 * result + accountId;
      result = 31 * result + (date != null ? date.hashCode() : 0);
      temp = Double.doubleToLongBits(amount);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + (isDebit ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "TransactionHS{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", accountId=" + accountId +
            ", date=" + date +
            ", amount=" + amount +
            ", isDebit=" + isDebit +
            '}';
   }
}

