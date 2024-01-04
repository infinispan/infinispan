package org.infinispan.protostream.sampledomain;

import java.math.BigDecimal;
import java.util.Date;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 */
public class Transaction {

   private int id;
   private String description;
   private String longDescription;
   private String notes;
   private int accountId;
   private Date date;
   private BigDecimal amount;
   private boolean isDebit;
   private boolean isValid;

   @Basic(projectable = true, sortable = true)
   @ProtoField(value = 1, defaultValue = "0")
   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   @Basic(sortable = true)
   @ProtoField(2)
   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }

   @Basic(projectable = true)
   @ProtoField(3)
   public String getLongDescription() {
      return longDescription;
   }

   public void setLongDescription(String longDescription) {
      this.longDescription = longDescription;
   }

   @Text(projectable = true, analyzer = "ngram")
   @ProtoField(4)
   public String getNotes() {
      return notes;
   }

   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Basic(projectable = true)
   @ProtoField(value = 5, defaultValue = "0")
   public int getAccountId() {
      return accountId;
   }

   public void setAccountId(int accountId) {
      this.accountId = accountId;
   }

   @Basic(projectable = true)
   @ProtoField(6)
   public Date getDate() {
      return date;
   }

   public void setDate(Date date) {
      this.date = date;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(value = 7, defaultValue = "0.0")
   public double getAmount() {
      return amount.doubleValue();
   }

   public void setAmount(double amount) {
      this.amount = new BigDecimal(amount);
   }

   @Basic(projectable = true)
   @ProtoField(value = 8, defaultValue = "false")
   public boolean isDebit() {
      return isDebit;
   }

   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @ProtoField(value = 9, defaultValue = "false")
   public boolean isValid() {
      return isValid;
   }

   public void setValid(boolean isValid) {
      this.isValid = isValid;
   }

   @Override
   public String toString() {
      return "Transaction{" +
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
