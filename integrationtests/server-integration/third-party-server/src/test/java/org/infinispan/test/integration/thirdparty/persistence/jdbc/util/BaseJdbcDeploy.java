package org.infinispan.test.integration.thirdparty.persistence.jdbc.util;

import org.jboss.shrinkwrap.api.spec.WebArchive;

import java.io.File;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;

public class BaseJdbcDeploy {

    public static void addJdbcLibraries(WebArchive war) {
        addLibrary(war, "org.infinispan:infinispan-cachestore-jdbc");
        addLibrary(war, "org.infinispan:infinispan-core");

        addLibrary(war, "org.testng:testng");
        addLibrary(war, "org.mockito:mockito-core");
        war.addAsResource("connection.properties");

        war.addAsLibrary(new File("target/test-libs/infinispan-core-tests.jar"))
                .addAsLibrary(new File("target/test-libs/infinispan-commons-test.jar"))
                .addAsLibrary(new File("target/test-libs/infinispan-cachestore-jdbc-tests.jar"))
                .addAsLibrary(new File("target/test-libs/infinispan-cachestore-jdbc-common-tests.jar"))
                .addAsLibrary(new File("target/test-libs/infinispan-component-annotations.jar"))
                .addAsLibrary(new File("target/test-libs/h2.jar"))
                .addAsLibrary(new File(System.getProperty("driver.path")));
    }
}
