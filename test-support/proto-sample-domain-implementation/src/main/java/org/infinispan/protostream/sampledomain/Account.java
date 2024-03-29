package org.infinispan.protostream.sampledomain;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoField;

//todo move everything to core and make a tests jar

/**
 * @author anistor@redhat.com
 */
@Indexed
public class Account {

   public enum Currency {
      @ProtoEnumValue(0)
      EUR,
      @ProtoEnumValue(1)
      GBP,
      @ProtoEnumValue(2)
      USD,
      @ProtoEnumValue(3)
      BRL
   }

   private int id;

   private String description;

   private Date creationDate;

   private Limits limits;

   private Limits hardLimits;

   private List<byte[]> blurb;

   private Currency[] currencies;

   public static class Limits {

      private Double maxDailyLimit;

      private Double maxTransactionLimit;

      private String[] payees = new String[0];

      @Basic(projectable = true)
      @ProtoField(1)
      public Double getMaxDailyLimit() {
         return maxDailyLimit;
      }

      public void setMaxDailyLimit(Double maxDailyLimit) {
         this.maxDailyLimit = maxDailyLimit;
      }

      @Basic(projectable = true)
      @ProtoField(2)
      public Double getMaxTransactionLimit() {
         return maxTransactionLimit;
      }

      public void setMaxTransactionLimit(Double maxTransactionLimit) {
         this.maxTransactionLimit = maxTransactionLimit;
      }

      @Basic(projectable = true)
      @ProtoField(3)
      public String[] getPayees() {
         return payees;
      }

      public void setPayees(String[] payees) {
         this.payees = payees;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Limits limits = (Limits) o;
         return Objects.equals(maxDailyLimit, limits.maxDailyLimit) &&
               Objects.equals(maxTransactionLimit, limits.maxTransactionLimit) &&
               Arrays.equals(payees, limits.payees);
      }

      @Override
      public int hashCode() {
         return Objects.hash(maxDailyLimit, maxTransactionLimit, Arrays.hashCode(payees));
      }

      @Override
      public String toString() {
         return "Limits{" +
               "maxDailyLimit=" + maxDailyLimit +
               ", maxTransactionLimit=" + maxTransactionLimit +
               ", payees=" + Arrays.toString(payees) +
               '}';
      }
   }

   public Account() {
      // hardLimits is a required field, so we make our life easy by providing defaults here
      hardLimits = new Limits();
      hardLimits.setMaxTransactionLimit(5000.0);
      hardLimits.setMaxDailyLimit(10000.0);
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(value = 1, defaultValue = "0")
   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(2)
   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(3)
   public Date getCreationDate() {
      return creationDate;
   }

   public void setCreationDate(Date creationDate) {
      this.creationDate = creationDate;
   }

   @Embedded
   @ProtoField(4)
   public Limits getLimits() {
      return limits;
   }

   public void setLimits(Limits limits) {
      this.limits = limits;
   }

   @Embedded
   @ProtoField(5)
   public Limits getHardLimits() {
      return hardLimits;
   }

   public void setHardLimits(Limits hardLimits) {
      this.hardLimits = hardLimits;
   }

   @Basic(projectable = true)
   @ProtoField(6)
   public List<byte[]> getBlurb() {
      return blurb;
   }

   public void setBlurb(List<byte[]> blurb) {
      this.blurb = blurb;
   }

   private boolean blurbEquals(List<byte[]> otherBlurbs) {
      if (otherBlurbs == blurb) {
         return true;
      }
      if (otherBlurbs == null || blurb == null || otherBlurbs.size() != blurb.size()) {
         return false;
      }
      for (int i = 0; i < blurb.size(); i++) {
         if (!Arrays.equals(blurb.get(i), otherBlurbs.get(i))) {
            return false;
         }
      }
      return true;
   }

   @ProtoField(7)
   public Currency[] getCurrencies() {
      return currencies;
   }

   public void setCurrencies(Currency[] currencies) {
      this.currencies = currencies;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Account account = (Account) o;
      return id == account.id &&
            Objects.equals(description, account.description) &&
            Objects.equals(creationDate, account.creationDate) &&
            Objects.equals(limits, account.limits) &&
            Objects.equals(hardLimits, account.hardLimits) &&
            blurbEquals(account.blurb) &&
            Arrays.equals(currencies, account.currencies);
   }

   @Override
   public int hashCode() {
      int blurbHash = 0;
      if (blurb != null) {
         for (byte[] b : blurb) {
            blurbHash = 31 * blurbHash + Arrays.hashCode(b);
         }
      }
      return Objects.hash(id, description, creationDate, limits, hardLimits, blurbHash, Arrays.hashCode(currencies));
   }

   @Override
   public String toString() {
      return "Account{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", creationDate='" + creationDate + '\'' +
            ", limits=" + limits +
            ", hardLimits=" + hardLimits +
            ", blurb=" + (blurb != null ? blurb.stream().map(Arrays::toString).collect(Collectors.toList()) : "null") +
            ", currencies=" + Arrays.toString(currencies) +
            '}';
   }
}
