package org.infinispan.configuration.global;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.audit.NullAuditLogger;
import org.infinispan.security.impl.CacheRoleImpl;

/**
 * GlobalAuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfiguration {
   private static final Map<String, Role> DEFAULT_ROLES;
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   public static final AttributeDefinition<AuditLogger> AUDIT_LOGGER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.AUDIT_LOGGER, (AuditLogger) new NullAuditLogger())
         .copier(identityCopier()).serializer(AttributeSerializer.INSTANCE_CLASS_NAME).immutable().build();
   public static final AttributeDefinition<Map<String, Role>> ROLES = AttributeDefinition.<Map<String, Role>>builder(org.infinispan.configuration.parsing.Attribute.ROLES, new HashMap<>())
         .initializer(new AttributeInitializer<Map<String, Role>>() {
            @Override
            public Map<String, Role> initialize() {
               return DEFAULT_ROLES;
            }
         }).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GlobalAuthorizationConfiguration.class, ENABLED, AUDIT_LOGGER, ROLES);
   }

   static {
      Map<String, Role> roles = new HashMap<>();
      roles.put("admin", new CacheRoleImpl("admin", true, AuthorizationPermission.ALL));
      roles.put("application", new CacheRoleImpl("application", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.ALL_WRITE,
            AuthorizationPermission.LISTEN,
            AuthorizationPermission.EXEC,
            AuthorizationPermission.MONITOR
      ));
      roles.put("deployer", new CacheRoleImpl("deployer", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.ALL_WRITE,
            AuthorizationPermission.LISTEN,
            AuthorizationPermission.EXEC,
            AuthorizationPermission.CREATE,
            AuthorizationPermission.MONITOR
      ));
      roles.put("observer", new CacheRoleImpl("observer", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.MONITOR
      ));
      roles.put("monitor", new CacheRoleImpl("monitor", true,
            AuthorizationPermission.MONITOR
      ));
      // Deprecated roles. Will be removed in Infinispan 16.0
      roles.put("___schema_manager", new CacheRoleImpl("___schema_manager", false,
            AuthorizationPermission.CREATE
      ));
      roles.put("___script_manager", new CacheRoleImpl("___script_manager", false,
            AuthorizationPermission.CREATE
      ));
      DEFAULT_ROLES = Collections.unmodifiableMap(roles);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<AuditLogger> auditLogger;
   private final Attribute<Map<String, Role>> roles;
   private final PrincipalRoleMapperConfiguration roleMapperConfiguration;

   private final AttributeSet attributes;

   public GlobalAuthorizationConfiguration(AttributeSet attributes, PrincipalRoleMapperConfiguration roleMapperConfiguration) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.auditLogger = attributes.attribute(AUDIT_LOGGER);
      this.roles = attributes.attribute(ROLES);
      this.roleMapperConfiguration = roleMapperConfiguration;
   }

   public boolean enabled() {
      return enabled.get();
   }

   public AuditLogger auditLogger() {
      return auditLogger.get();
   }

   public PrincipalRoleMapper principalRoleMapper() {
      return roleMapperConfiguration.roleMapper();
   }

   public PrincipalRoleMapperConfiguration roleMapperConfiguration() {
      return roleMapperConfiguration;
   }

   public Map<String, Role> roles() {
      return roles.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "GlobalAuthorizationConfiguration{" +
            "roleMapperConfiguration=" + roleMapperConfiguration +
            ", attributes=" + attributes +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalAuthorizationConfiguration that = (GlobalAuthorizationConfiguration) o;

      if (roleMapperConfiguration != null ? !roleMapperConfiguration.equals(that.roleMapperConfiguration) : that.roleMapperConfiguration != null)
         return false;
      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      int result = roleMapperConfiguration != null ? roleMapperConfiguration.hashCode() : 0;
      result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
      return result;
   }
}
