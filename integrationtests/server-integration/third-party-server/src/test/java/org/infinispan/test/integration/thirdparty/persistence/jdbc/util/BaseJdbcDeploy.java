package org.infinispan.test.integration.thirdparty.persistence.jdbc.util;

import static org.infinispan.test.integration.GenericDeploymentHelper.addLibrary;

import java.io.File;

import org.jboss.shrinkwrap.api.spec.WebArchive;

public class BaseJdbcDeploy {

    public static void addJdbcLibraries(WebArchive war) {
        addLibrary(war, "org.infinispan:infinispan-cachestore-jdbc");
        addLibrary(war, "org.infinispan:infinispan-core");
        addLibrary(war, "com.h2database:h2");
        addLibrary(war, "org.postgresql:postgresql");
        addLibrary(war, "com.mysql:mysql-connector-j");
        addLibrary(war, "com.oracle.database.jdbc:ojdbc11");
        addLibrary(war, "org.mariadb.jdbc:mariadb-java-client");
        addLibrary(war, "com.ibm.db2:jcc");
        addLibrary(war, "com.microsoft.sqlserver:mssql-jdbc");
        addLibrary(war, "org.jgroups:jgroups");
        war.addAsResource("connection.properties");
        String driverPath = System.getProperty("driver.path");
        if (driverPath != null) {
            File driverFile = new File(driverPath);
            if (driverFile.exists()) {
                war.addAsLibrary(driverFile);
            }
        }
    }
}
