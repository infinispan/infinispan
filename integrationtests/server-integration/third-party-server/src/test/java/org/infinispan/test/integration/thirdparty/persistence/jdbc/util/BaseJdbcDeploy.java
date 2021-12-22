package org.infinispan.test.integration.thirdparty.persistence.jdbc.util;

import org.jboss.shrinkwrap.api.spec.WebArchive;

import java.io.File;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;

public class BaseJdbcDeploy {

    public static void addJdbcLibraries(WebArchive war) {
        addLibrary(war, "org.infinispan:infinispan-cachestore-jdbc");
        addLibrary(war, "org.infinispan:infinispan-core");
        addLibrary(war, "com.h2database:h2");
        addLibrary(war, "org.jgroups:jgroups");
        war.addAsResource("connection.properties");
        war.addAsLibrary(new File(System.getProperty("driver.path")));
    }
}
