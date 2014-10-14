package org.infinispan.cli.interpreter.statement;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * DenyStatement removes a role mapping from a user
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class DenyStatement implements Statement {
   private static final Log log = LogFactory.getLog(DenyStatement.class, Log.class);

   private final String principalName;
   private final String roleName;

   public DenyStatement(String roleName, String principalName) {
      this.roleName = roleName;
      this.principalName = principalName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      GlobalAuthorizationConfiguration gac = session.getCacheManager().getCacheManagerConfiguration().security().authorization();
      if (!gac.enabled()) {
         throw log.authorizationNotEnabledOnContainer();
      }
      if (!(gac.principalRoleMapper() instanceof ClusterRoleMapper)) {
         throw log.noClusterPrincipalMapper("DENY");
      }
      ClusterRoleMapper cpm = (ClusterRoleMapper) gac.principalRoleMapper();
      cpm.deny(roleName, principalName);
      return EmptyResult.RESULT;
   }

}
