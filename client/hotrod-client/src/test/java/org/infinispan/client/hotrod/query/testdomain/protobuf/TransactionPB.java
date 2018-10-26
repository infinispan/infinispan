package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.math.BigDecimal;
import java.util.Date;

import org.infinispan.query.dsl.embedded.testdomain.Transaction;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class TransactionPB implements Transaction {

   private int id;
   private String description;
   private String longDescription;
   private String notes;
   private int accountId;
   private Date date;
   private BigDecimal amount;
   private boolean isDebit;
   private boolean isValid;

   @Override
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   public String getDescription() {
      return description;
   }

   @Override
   public void setDescription(String description) {
      this.description = description;
   }

   @Override
   public String getLongDescription() {
      return longDescription;
   }

   @Override
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   public void setLongDescription(String longDescription) {
      this.longDescription = longDescription;
   }

   @Override
   public int getAccountId() {
      return accountId;
   }

   @Override
   public void setAccountId(int accountId) {
      this.accountId = accountId;
   }

   @Override
   public Date getDate() {
      return date;
   }

   @Override
   public void setDate(Date date) {
      this.date = date;
   }

   @Override
   public double getAmount() {
      return amount.doubleValue();
   }

   @Override
   public void setAmount(double amount) {
      this.amount = new BigDecimal(amount);
   }

   @Override
   public boolean isDebit() {
      return isDebit;
   }

   @Override
   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @Override
   public boolean isValid() {
      return isValid;
   }

   @Override
   public void setValid(boolean isValid) {
      this.isValid = isValid;
   }

   @Override
   public String toString() {
      return "TransactionPB{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", longDescription='" + longDescription + '\'' +
            ", notes='" + notes + '\'' +
            ", accountId=" + accountId +
            ", date='" + date + '\'' +
            ", amount=" + amount +
            ", isDebit=" + isDebit +
            ", isValid=" + isValid +
            '}';
   }
}
