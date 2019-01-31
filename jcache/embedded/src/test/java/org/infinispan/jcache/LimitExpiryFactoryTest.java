package org.infinispan.jcache.embedded;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;

import org.junit.Test;

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
