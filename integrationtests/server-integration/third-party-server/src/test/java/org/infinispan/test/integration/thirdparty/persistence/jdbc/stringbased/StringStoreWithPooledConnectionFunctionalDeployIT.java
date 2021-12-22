package org.infinispan.test.integration.thirdparty.persistence.jdbc.stringbased;

import org.infinispan.persistence.jdbc.stringbased.StringStoreWithPooledConnectionFunctionalTest;
import org.infinispan.test.integration.persistence.jdbc.stringbased.StringStoreWithPooledConnectionFunctionalIT;
import org.infinispan.test.integration.persistence.jdbc.util.DatabaseManager;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;

import static org.infinispan.test.integration.thirdparty.persistence.jdbc.util.BaseJdbcDeploy.addJdbcLibraries;

public class StringStoreWithPooledConnectionFunctionalDeployIT extends StringStoreWithPooledConnectionFunctionalIT {

    @Deployment
    @TargetsContainer("server-1")
    public static Archive<?> deployment() {
        WebArchive war = DeploymentHelper.createDeployment();
        war.addClass(StringStoreWithPooledConnectionFunctionalIT.class);
        war.addClass(StringStoreWithPooledConnectionFunctionalTest.class);
        war.addClass(DatabaseManager.class);
        addJdbcLibraries(war);
        return war;
    }

}
