package org.infinispan.query.indexedembedded;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
@Indexed
public class Country implements Serializable {

   public Long id;

   public @Field String countryName;

   public final @IndexedEmbedded Set<City> cities = new HashSet<City>();

}
