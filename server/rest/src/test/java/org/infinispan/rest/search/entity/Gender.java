package org.infinispan.rest.search.entity;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public enum Gender implements Serializable, ExternalPojo {
   MALE,
   FEMALE
}
