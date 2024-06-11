package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.math.BigDecimal;
import java.util.Date;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
@ProtoName("Transaction")
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
   @Basic(projectable = true, sortable = true)
   @ProtoField(value = 1, defaultValue = "0")
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   @Basic(sortable = true)
   @ProtoField(2)
   public String getDescription() {
      return description;
   }

   @Override
   public void setDescription(String description) {
      this.description = description;
   }

   @Override
   @Text(projectable = true)
   @ProtoField(3)
   public String getLongDescription() {
      return longDescription;
   }

   @Override
   public void setLongDescription(String longDescription) {
      this.longDescription = longDescription;
   }

   @Override
   @Text(projectable = true, analyzer = "ngram")
   @ProtoField(4)
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(value = 5, defaultValue = "0")
   public int getAccountId() {
      return accountId;
   }

   @Override
   public void setAccountId(int accountId) {
      this.accountId = accountId;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(6)
   public Date getDate() {
      return date;
   }

   @Override
   public void setDate(Date date) {
      this.date = date;
   }

   @Override
   @Basic(projectable = true, sortable = true)
   @ProtoField(value = 7, defaultValue = "0.0")
   public double getAmount() {
      return amount.doubleValue();
   }

   @Override
   public void setAmount(double amount) {
      this.amount = new BigDecimal(amount);
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(value = 8, defaultValue = "false", name = "isDebit")
   public boolean isDebit() {
      return isDebit;
   }

   @Override
   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @Override
   @ProtoField(value = 9, defaultValue = "false", name = "isValid")
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
