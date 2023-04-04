package org.infinispan.quarkus.embedded;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;

/**
 * @author William Burns
 */
@QuarkusTestResource(InfinispanEmbeddedTestResource.class)
@QuarkusIntegrationTest
public class InfinispanEmbeddedFunctionalityInGraalITCase extends InfinispanEmbeddedFunctionalityTest {

}
