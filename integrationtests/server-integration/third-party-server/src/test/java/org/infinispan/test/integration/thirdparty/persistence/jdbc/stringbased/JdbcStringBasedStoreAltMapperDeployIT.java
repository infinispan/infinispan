package org.infinispan.test.integration.thirdparty.persistence.jdbc.stringbased;

import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStoreAltMapperTest;
import org.infinispan.test.integration.persistence.jdbc.stringbased.JdbcStringBasedStoreAltMapperIT;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

import static org.infinispan.test.integration.thirdparty.persistence.jdbc.util.BaseJdbcDeploy.addJdbcLibraries;

@RunWith(Arquillian.class)
public class JdbcStringBasedStoreAltMapperDeployIT extends JdbcStringBasedStoreAltMapperIT {

    @Deployment
    @TargetsContainer("server-1")
    public static Archive<?> deployment() {
        WebArchive war = DeploymentHelper.createDeployment();
        war.addClass(JdbcStringBasedStoreAltMapperIT.class);
        war.addClass(JdbcStringBasedStoreAltMapperTest.class);
        war.addClass(DatabaseManager.class);
        addJdbcLibraries(war);
        return war;
    }
}
