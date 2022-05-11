package org.infinispan.query.indexedembedded;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
@Indexed
public class Country {

   @ProtoField(1)
   public Long id;

   @ProtoField(2)
   public String countryName;

   @ProtoField(number = 3, collectionImplementation = HashSet.class)
   public Set<City> cities = new HashSet<>();

   @Text
   public String getCountryName() {
      return countryName;
   }

   @Embedded
   public Set<City> getCities() {
      return cities;
   }
}
