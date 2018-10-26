package org.infinispan.all.embeddedquery.testdomain;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Account extends Serializable {

   enum Currency {
      EUR, GBP, USD, BRL
   }

   int getId();

   void setId(int id);

   String getDescription();

   void setDescription(String description);

   Date getCreationDate();

   void setCreationDate(Date creationDate);

   Limits getLimits();

   void setLimits(Limits limits);

   Limits getHardLimits();

   void setHardLimits(Limits hardLimits);

   List<byte[]> getBlurb();

   void setBlurb(List<byte[]> blurb);

   Currency[] getCurrencies();

   void setCurrencies(Currency[] currencies);
}
