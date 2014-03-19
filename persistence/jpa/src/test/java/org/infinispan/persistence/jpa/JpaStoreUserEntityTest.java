package org.infinispan.persistence.jpa;

import org.infinispan.persistence.jpa.entity.User;
import org.testng.annotations.Test;

/**
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "persistence.JpaStoreUserEntityTest")
public class JpaStoreUserEntityTest extends BaseJpaStoreTest {
   @Override
   protected Class<?> getEntityClass() {
      return User.class;
   }

   @Override
	protected TestObject createTestObject(String suffix) {
		User user = new User();
		user.setUsername("u_" + suffix);
		user.setFirstName("fn_" + suffix);
		user.setLastName("ln_" + suffix);
		user.setNote("Some notes " + suffix);

		return new TestObject(user.getUsername(), user);
	}
}
