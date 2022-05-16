package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Analyzer;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.Indexed;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
public class TransactionHS implements Transaction, Serializable {

   private int id;

   private String description;

   private String longDescription;

   private String notes;

   private int accountId;

   private Date date;

   private double amount;

   private boolean isDebit;

   // not indexed!
   private boolean isValid;

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   @ProtoField(number = 1, defaultValue = "0")
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   @ProtoField(number = 2)
   public String getDescription() {
      return description;
   }

   @Override
   public void setDescription(String description) {
      this.description = description;
   }

   @Override
   @Field
   @ProtoField(number = 3)
   public String getLongDescription() {
      return longDescription;
   }

   @Override
   public void setLongDescription(String longDescription) {
      this.longDescription = longDescription;
   }

   @Override
   @Field(analyze = Analyze.YES)
   @Analyzer(definition = "ngram")
   @ProtoField(number = 4)
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @ProtoField(number = 5, defaultValue = "0")
   public int getAccountId() {
      return accountId;
   }

   @Override
   public void setAccountId(int accountId) {
      this.accountId = accountId;
   }

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @ProtoField(number = 6)
   public Date getDate() {
      return date;
   }

   @Override
   public void setDate(Date date) {
      this.date = date;
   }

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   @ProtoField(number = 7, defaultValue = "0")
   public double getAmount() {
      return amount;
   }

   @Override
   public void setAmount(double amount) {
      this.amount = amount;
   }

   @Override
   @Field(store = Store.YES, analyze = Analyze.NO)
   @ProtoField(number = 8, defaultValue = "false")
   public boolean isDebit() {
      return isDebit;
   }

   @Override
   public void setDebit(boolean isDebit) {
      this.isDebit = isDebit;
   }

   @Override
   @ProtoField(number = 9, defaultValue = "false")
   public boolean isValid() {
      return isValid;
   }

   @Override
   public void setValid(boolean isValid) {
      this.isValid = isValid;
   }

   @Override
   public String toString() {
      return "TransactionHS{" +
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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TransactionHS that = (TransactionHS) o;
      return id == that.id && accountId == that.accountId && Double.compare(that.amount, amount) == 0 && isDebit == that.isDebit && isValid == that.isValid &&
            Objects.equals(description, that.description) && Objects.equals(longDescription, that.longDescription) && Objects.equals(notes, that.notes) && Objects.equals(date, that.date);
   }

   @Override
   public int hashCode() {
      return Objects.hash(id, description, longDescription, notes, accountId, date, amount, isDebit, isValid);
   }
}
