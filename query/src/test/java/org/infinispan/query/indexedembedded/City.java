package org.infinispan.query.indexedembedded;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class City implements Serializable {

   public @Field String name;

}
