package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.io.Serializable;
import java.util.Date;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Analyzer;
import org.hibernate.search.annotations.DateBridge;
import org.hibernate.search.annotations.EncodingType;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.NumericField;
import org.hibernate.search.annotations.Resolution;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
@Analyzer(definition = "standard")
public class TransactionHS implements Transaction, Serializable, ExternalPojo {

   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   private int id;

   @Field(store = Store.NO, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   @SortableField
   private String description;

   @Field
   @SortableField
   private String longDescription;

   @Field
   @Analyzer(definition = "ngram")
   private String notes;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int accountId;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @DateBridge(encoding = EncodingType.STRING, resolution = Resolution.MILLISECOND)
   private Date date;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @NumericField
   @SortableField
   private double amount;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private boolean isDebit;

   // not indexed!
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
   public String getLongDescription() {
      return longDescription;
   }

   @Override
   public void setLongDescription(String longDescription) {
      this.longDescription = longDescription;
   }

   @Override
   public void setDescription(String description) {
      this.description = description;
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
      return amount;
   }

   @Override
   public void setAmount(double amount) {
      this.amount = amount;
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransactionHS other = (TransactionHS) o;

      if (accountId != other.accountId) return false;
      if (Double.compare(other.amount, amount) != 0) return false;
      if (id != other.id) return false;
      if (isDebit != other.isDebit) return false;
      if (isValid != other.isValid) return false;
      if (date != null ? !date.equals(other.date) : other.date != null) return false;
      if (description != null ? !description.equals(other.description) : other.description != null) return false;
      if (longDescription != null ? !longDescription.equals(other.longDescription) : other.longDescription != null)
         return false;
      if (notes != null ? !notes.equals(other.notes) : other.notes != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + (description != null ? description.hashCode() : 0);
      result = 31 * result + (longDescription != null ? longDescription.hashCode() : 0);
      result = 31 * result + (notes != null ? notes.hashCode() : 0);
      result = 31 * result + accountId;
      result = 31 * result + (date != null ? date.hashCode() : 0);
      long temp = Double.doubleToLongBits(amount);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + (isDebit ? 1 : 0);
      result = 31 * result + (isValid ? 1 : 0);
      return result;
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
}
