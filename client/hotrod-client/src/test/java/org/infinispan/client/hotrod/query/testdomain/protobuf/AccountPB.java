package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.Limits;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AccountPB implements Account {

   private int id;

   private String description;

   private Date creationDate;

   private Limits limits;

   private Limits hardLimits;

   // not indexed
   private List<byte[]> blurb = new ArrayList<>();

   private Currency[] currencies = new Currency[0];

   public AccountPB() {
      // hardLimits is a required field, so we make our life easy by providing defaults here
      hardLimits = new LimitsPB();
      hardLimits.setMaxTransactionLimit(5000.0);
      hardLimits.setMaxDailyLimit(10000.0);
   }

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
   public Date getCreationDate() {
      return creationDate;
   }

   @Override
   public void setCreationDate(Date creationDate) {
      this.creationDate = creationDate;
   }

   @Override
   public Limits getLimits() {
      return limits;
   }

   @Override
   public void setLimits(Limits limits) {
      this.limits = limits;
   }

   @Override
   public Limits getHardLimits() {
      return hardLimits;
   }

   @Override
   public void setHardLimits(Limits hardLimits) {
      this.hardLimits = hardLimits;
   }

   @Override
   public List<byte[]> getBlurb() {
      return blurb;
   }

   @Override
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

   @Override
   public Currency[] getCurrencies() {
      return currencies;
   }

   @Override
   public void setCurrencies(Currency[] currencies) {
      this.currencies = currencies;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AccountPB account = (AccountPB) o;
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
      return "AccountPB{" +
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
