package org.infinispan.test.integration.thirdparty.persistence.jdbc;

import org.infinispan.persistence.jdbc.PooledConnectionFactoryTest;
import org.infinispan.test.integration.persistence.jdbc.PooledConnectionFactoryIT;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;

import static org.infinispan.test.integration.thirdparty.persistence.jdbc.util.BaseJdbcDeploy.addJdbcLibraries;

public class PooledConnectionFactoryDeployIT extends PooledConnectionFactoryIT {

    @Deployment
    @TargetsContainer("server-1")
    public static Archive<?> deployment() {
        WebArchive war = DeploymentHelper.createDeployment();
        war.addClass(PooledConnectionFactoryIT.class);
        war.addClass(PooledConnectionFactoryTest.class);
        war.addClass(DatabaseManager.class);
        addJdbcLibraries(war);
        return war;
    }

}
