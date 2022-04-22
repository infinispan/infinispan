package org.infinispan.test.integration.thirdparty.persistence.jdbc.stringbased;

import org.infinispan.test.integration.persistence.jdbc.stringbased.PooledConnectionIT;
import org.infinispan.test.integration.persistence.jdbc.util.JdbcConfigurationUtil;
import org.infinispan.test.integration.persistence.jdbc.util.TableManipulation;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.runner.RunWith;

import static org.infinispan.test.integration.thirdparty.persistence.jdbc.util.BaseJdbcDeploy.addJdbcLibraries;

@RunWith(Arquillian.class)
public class PooledConnectionDeployIT extends PooledConnectionIT {

    @Deployment
    @TargetsContainer("server-1")
    public static Archive<?> deployment() {
        WebArchive war = DeploymentHelper.createDeployment();
        war.addClass(PooledConnectionIT.class);
        war.addClass(TableManipulation.class);
        war.addClass(JdbcConfigurationUtil.class);
        addJdbcLibraries(war);
        return war;
    }

}
