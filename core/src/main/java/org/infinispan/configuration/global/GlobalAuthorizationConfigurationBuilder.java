package org.infinispan.configuration.global;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * GlobalAuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalAuthorizationConfiguration> {
   public static final Log log = LogFactory.getLog(GlobalAuthorizationConfigurationBuilder.class);
   private boolean enabled = false;
   private PrincipalRoleMapper principalRoleMapper;
   private Map<String, GlobalRoleConfigurationBuilder> roles = new HashMap<String, GlobalRoleConfigurationBuilder>();

   public GlobalAuthorizationConfigurationBuilder(GlobalSecurityConfigurationBuilder builder) {
      super(builder.getGlobalConfig());
   }

   public GlobalAuthorizationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public GlobalAuthorizationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public GlobalAuthorizationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * The class of a mapper which converts the {@link Principal}s associated with a {@link Subject} into a set of roles
    *
    * @param principalRoleMapper
    */
   public GlobalAuthorizationConfigurationBuilder principalRoleMapper(PrincipalRoleMapper principalRoleMapper) {
      this.principalRoleMapper = principalRoleMapper;
      return this;
   }

   public GlobalRoleConfigurationBuilder role(String name) {
      GlobalRoleConfigurationBuilder role = new GlobalRoleConfigurationBuilder(name, this);
      roles.put(name, role);
      return role;
   }

   @Override
   public void validate() {
      if (principalRoleMapper == null) {
         throw log.invalidPrincipalRoleMapper();
      }
   }

   @Override
   public GlobalAuthorizationConfiguration create() {
      Map<String, Role> rolesCfg = new HashMap<String, Role>();
      for(GlobalRoleConfigurationBuilder role : this.roles.values()) {
         Role roleCfg = role.create();
         rolesCfg.put(roleCfg.getName(), roleCfg);
      }
      return new GlobalAuthorizationConfiguration(enabled, principalRoleMapper, rolesCfg);
   }

   @Override
   public Builder<?> read(GlobalAuthorizationConfiguration template) {
      this.enabled = template.enabled();
      this.principalRoleMapper = template.principalRoleMapper();
      this.roles.clear();
      for(Role role : template.roles().values()) {
         this.role(role.getName()).read(role);
      }
      return this;
   }

}
