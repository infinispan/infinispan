package org.infinispan.jcache;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.TimeUnit;

import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.jcache.embedded.LimitExpiryFactory;
import org.testng.annotations.Test;

/**
 * Test preventing controlling the flow with NPE in LimitExpiryPolicy methods
 *
 * <p>See ISPN-9912</p>
 */
@Test(groups = "unit", testName = "jcache.LimitExpiryFactoryTest")
public class LimitExpiryFactoryTest {

    @Test
    public void getExpiryForAccess() {

        ExpiryPolicy limitExpiryPolicy = new LimitExpiryFactory(EternalExpiryPolicy.factoryOf(), 0, 0).create();

        assertEquals(null, limitExpiryPolicy.getExpiryForAccess());
    }

    @Test
    public void getExpiryForCreation() {

        ExpiryPolicy limitExpiryPolicy = new LimitExpiryFactory(EternalExpiryPolicy.factoryOf(), 0, 0).create();

        assertEquals(new Duration(TimeUnit.MILLISECONDS, 0), limitExpiryPolicy.getExpiryForCreation());
    }

    @Test
    public void getExpiryForUpdate() {

        ExpiryPolicy limitExpiryPolicy = new LimitExpiryFactory(EternalExpiryPolicy.factoryOf(), 0, 0).create();

        assertEquals(null, limitExpiryPolicy.getExpiryForUpdate());
    }
}
