package org.infinispan.query.indexedembedded;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.infinispan.marshall.core.ExternalPojo;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 */
@Indexed
public class Country implements Serializable, ExternalPojo {

   public Long id;

   public @Field String countryName;

   public final @IndexedEmbedded Set<City> cities = new HashSet<City>();

}
