package org.infinispan.test.hibernate.cache.commons.functional.entities;

import jakarta.persistence.Cacheable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

/**
 * The class should be in a package that is different from the test
 * so that the test does not have access to the private ID field.
 *
 * @author Gail Badner
 */
@Entity
@Cacheable
public class WithSimpleId {
	@Id
	private Long id;
}
