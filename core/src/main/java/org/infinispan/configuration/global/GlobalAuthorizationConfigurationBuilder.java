package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.GlobalAuthorizationConfiguration.AUDIT_LOGGER;
import static org.infinispan.configuration.global.GlobalAuthorizationConfiguration.ENABLED;
import static org.infinispan.configuration.global.GlobalAuthorizationConfiguration.ROLES;
import static org.infinispan.util.logging.Log.CONFIG;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.security.audit.LoggingAuditLogger;
import org.infinispan.security.audit.NullAuditLogger;

/**
 * GlobalAuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalAuthorizationConfiguration> {
   private final AttributeSet attributes;
   private final PrincipalRoleMapperConfigurationBuilder roleMapper;
   private final RolePermissionMapperConfigurationBuilder permissionMapper;
   private final Map<String, GlobalRoleConfigurationBuilder> roles = new HashMap<>();

   public GlobalAuthorizationConfigurationBuilder(GlobalSecurityConfigurationBuilder builder) {
      super(builder.getGlobalConfig());
      roleMapper = new PrincipalRoleMapperConfigurationBuilder(getGlobalConfig());
      permissionMapper = new RolePermissionMapperConfigurationBuilder(getGlobalConfig());
      attributes = GlobalAuthorizationConfiguration.attributeDefinitionSet();
   }

   public GlobalAuthorizationConfigurationBuilder enable() {
      return this.enabled(true);
   }

   public GlobalAuthorizationConfigurationBuilder disable() {
      return this.enabled(false);
   }

   public GlobalAuthorizationConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public boolean isEnabled() {
      return attributes.attribute(ENABLED).get();
   }

   /**
    * The instance of an {@link AuditLogger} to be used to track operations performed on caches and cachemanagers. The default logger is
    * the {@link NullAuditLogger}. You can also use the {@link LoggingAuditLogger} which will send audit messages to the log.
    * @param auditLogger
    */
   public GlobalAuthorizationConfigurationBuilder auditLogger(AuditLogger auditLogger) {
      attributes.attribute(AUDIT_LOGGER).set(auditLogger);
      return this;
   }

   /**
    * The class of a mapper which converts the {@link Principal}s associated with a {@link Subject} into a set of roles
    *
    * @param principalRoleMapper
    */
   public GlobalAuthorizationConfigurationBuilder principalRoleMapper(PrincipalRoleMapper principalRoleMapper) {
      roleMapper.mapper(principalRoleMapper);
      return this;
   }

   /**
    * The class of a mapper which maps {@link Role}s to {@link org.infinispan.security.AuthorizationPermission}s
    *
    * @param rolePermissionMapper
    */
   public GlobalAuthorizationConfigurationBuilder rolePermissionMapper(RolePermissionMapper rolePermissionMapper) {
      permissionMapper.mapper(rolePermissionMapper);
      return this;
   }

   public GlobalRoleConfigurationBuilder role(String name) {
      GlobalRoleConfigurationBuilder role = new GlobalRoleConfigurationBuilder(name, this);
      roles.put(name, role);
      return role;
   }

   @Override
   public void validate() {
      roleMapper.validate();
      if (attributes.attribute(ENABLED).get() && roleMapper.mapper() == null) {
         throw CONFIG.invalidPrincipalRoleMapper();
      }
   }

   @Override
   public GlobalAuthorizationConfiguration create() {
      Map<String, Role> rolesCfg = new HashMap<>();
      for(GlobalRoleConfigurationBuilder role : this.roles.values()) {
         Role roleCfg = role.create();
         rolesCfg.put(roleCfg.getName(), roleCfg);
      }
      if (!rolesCfg.isEmpty()) attributes.attribute(ROLES).set(rolesCfg);
      return new GlobalAuthorizationConfiguration(attributes.protect(), roleMapper.create(), permissionMapper.create());
   }

   @Override
   public Builder<?> read(GlobalAuthorizationConfiguration template) {
      attributes.read(template.attributes());
      this.roleMapper.read(template.roleMapperConfiguration());
      this.permissionMapper.read(template.permissionMapperConfiguration());
      this.roles.clear();
      for(Role role : template.roles().values()) {
         this.role(role.getName()).read(role);
      }
      return this;
   }
}
