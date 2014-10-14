package org.infinispan.cli.interpreter.statement;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * RolesStatement lists the roles of a user
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class RolesStatement implements Statement {
   private static final Log log = LogFactory.getLog(RolesStatement.class, Log.class);

   private final String principalName;

   public RolesStatement(String principalName) {
      this.principalName = principalName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      GlobalAuthorizationConfiguration gac = session.getCacheManager().getCacheManagerConfiguration().security().authorization();
      if (!gac.enabled()) {
         throw log.authorizationNotEnabledOnContainer();
      }
      if (!(gac.principalRoleMapper() instanceof ClusterRoleMapper)) {
         throw log.noClusterPrincipalMapper("ROLES");
      }
      ClusterRoleMapper cpm = (ClusterRoleMapper) gac.principalRoleMapper();
      if (principalName != null) {
         return new StringResult(cpm.list(principalName).toString());
      } else {
         return new StringResult(cpm.listAll());
      }
   }

}
