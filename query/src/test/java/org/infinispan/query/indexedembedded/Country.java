package org.infinispan.query.indexedembedded;

import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
@Indexed
public class Country {

   @ProtoField(number = 1)
   public Long id;

   @ProtoField(number = 2)
   public @Field String countryName;

   @ProtoField(number = 3, collectionImplementation = HashSet.class)
   public @IndexedEmbedded Set<City> cities = new HashSet<>();

}
