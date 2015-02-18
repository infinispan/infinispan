package org.infinispan.configuration.global;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.impl.LoggingAuditLogger;
import org.infinispan.security.impl.NullAuditLogger;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.configuration.global.GlobalAuthorizationConfiguration.*;

/**
 * GlobalAuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<GlobalAuthorizationConfiguration> {
   public static final Log log = LogFactory.getLog(GlobalAuthorizationConfigurationBuilder.class);
   private final AttributeSet attributes;
   private final Map<String, GlobalRoleConfigurationBuilder> roles = new HashMap<String, GlobalRoleConfigurationBuilder>();

   public GlobalAuthorizationConfigurationBuilder(GlobalSecurityConfigurationBuilder builder) {
      super(builder.getGlobalConfig());
      attributes = GlobalAuthorizationConfiguration.attributeDefinitionSet();
   }

   public GlobalAuthorizationConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public GlobalAuthorizationConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public GlobalAuthorizationConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
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
      attributes.attribute(PRINCIPAL_ROLE_MAPPER).set(principalRoleMapper);
      return this;
   }

   public GlobalRoleConfigurationBuilder role(String name) {
      GlobalRoleConfigurationBuilder role = new GlobalRoleConfigurationBuilder(name, this);
      roles.put(name, role);
      return role;
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get() && attributes.attribute(PRINCIPAL_ROLE_MAPPER).get() == null) {
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
      attributes.attribute(ROLES).set(rolesCfg);
      return new GlobalAuthorizationConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(GlobalAuthorizationConfiguration template) {
      attributes.read(template.attributes());
      this.roles.clear();
      for(Role role : template.roles().values()) {
         this.role(role.getName()).read(role);
      }
      return this;
   }

}
