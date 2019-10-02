package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Account extends Serializable {

   enum Currency {
      @ProtoEnumValue(number = 0)
      EUR,
      @ProtoEnumValue(number = 1)
      GBP,
      @ProtoEnumValue(number = 2)
      USD,
      @ProtoEnumValue(number = 3)
      BRL
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
