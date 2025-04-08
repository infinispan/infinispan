package org.infinispan.test.hibernate.cache.commons.functional.entities;

import jakarta.persistence.Cacheable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;

/**
 * The class should be in a package that is different from the test
 * so that the test does not have access to the private embedded ID.
 *
 * @author Gail Badner
 */
@Entity
@Cacheable
public class WithEmbeddedId {
	@EmbeddedId
	private PK embeddedId;
}
