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
 * GrantStatement adds a role mapping to a user
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GrantStatement implements Statement {
   private static final Log log = LogFactory.getLog(GrantStatement.class, Log.class);

   private final String principalName;
   private final String roleName;

   public GrantStatement(String roleName, String principalName) {
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
         throw log.noClusterPrincipalMapper("GRANT");
      }
      ClusterRoleMapper cpm = (ClusterRoleMapper) gac.principalRoleMapper();
      cpm.grant(roleName, principalName);
      return EmptyResult.RESULT;
   }

}
