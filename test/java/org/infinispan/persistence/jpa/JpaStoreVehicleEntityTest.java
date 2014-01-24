package org.infinispan.persistence.jpa;

import org.infinispan.persistence.jpa.entity.Vehicle;
import org.infinispan.persistence.jpa.entity.VehicleId;
import org.testng.annotations.Test;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
@Test (groups = "functional", testName = "persistence.JpaStoreVehicleEntityTest")
public class JpaStoreVehicleEntityTest extends BaseJpaStoreTest {
   @Override
   protected Class<?> getEntityClass() {
      return Vehicle.class;
   }

   @Override
	protected TestObject createTestObject(String key) {
		VehicleId id = new VehicleId("CA" + key, key);
		Vehicle v = new Vehicle();
		v.setId(id);
		v.setColor("c_" + key);
		
		return new TestObject(v.getId(), v);
	}
}
