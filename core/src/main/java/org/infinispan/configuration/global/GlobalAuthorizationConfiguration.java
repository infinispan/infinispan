package org.infinispan.configuration.global;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.security.audit.NullAuditLogger;

/**
 * GlobalAuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfiguration {
   public static final Map<String, Role> DEFAULT_ROLES;
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
      roles.put("admin", Role.newRole("admin", true, AuthorizationPermission.ALL));
      roles.put("application", Role.newRole("application", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.ALL_WRITE,
            AuthorizationPermission.LISTEN,
            AuthorizationPermission.EXEC,
            AuthorizationPermission.MONITOR
      ));
      roles.put("deployer", Role.newRole("deployer", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.ALL_WRITE,
            AuthorizationPermission.LISTEN,
            AuthorizationPermission.EXEC,
            AuthorizationPermission.CREATE,
            AuthorizationPermission.MONITOR
      ));
      roles.put("observer", Role.newRole("observer", true,
            AuthorizationPermission.ALL_READ,
            AuthorizationPermission.MONITOR
      ));
      roles.put("monitor", Role.newRole("monitor", true,
            AuthorizationPermission.MONITOR
      ));
      // Deprecated roles. Will be removed in Infinispan 16.0
      roles.put("___schema_manager", Role.newRole("___schema_manager", false,
            AuthorizationPermission.CREATE
      ));
      roles.put("___script_manager", Role.newRole("___script_manager", false,
            AuthorizationPermission.CREATE
      ));
      DEFAULT_ROLES = Collections.unmodifiableMap(roles);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<AuditLogger> auditLogger;
   private final Map<String, Role> roles;
   private final PrincipalRoleMapperConfiguration roleMapperConfiguration;
   private final RolePermissionMapperConfiguration permissionMapperConfiguration;
   private final RolePermissionMapper rolePermissionMapper;

   private final AttributeSet attributes;

   public GlobalAuthorizationConfiguration(AttributeSet attributes, PrincipalRoleMapperConfiguration roleMapperConfiguration, RolePermissionMapperConfiguration permissionMapperConfiguration) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.auditLogger = attributes.attribute(AUDIT_LOGGER);
      this.roles = attributes.attribute(ROLES).get();
      this.roleMapperConfiguration = roleMapperConfiguration;
      this.permissionMapperConfiguration = permissionMapperConfiguration;
      this.rolePermissionMapper = permissionMapperConfiguration.permissionMapper();
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

   public RolePermissionMapper rolePermissionMapper() {
      return rolePermissionMapper;
   }

   public PrincipalRoleMapperConfiguration roleMapperConfiguration() {
      return roleMapperConfiguration;
   }

   public boolean isDefaultRoles() {
      return roles == DEFAULT_ROLES;
   }

   public RolePermissionMapperConfiguration permissionMapperConfiguration() {
      return permissionMapperConfiguration;
   }

   public Map<String, Role> roles() {
      Map<String, Role> all = new HashMap<>(roles);
      if (rolePermissionMapper != null) {
         all.putAll(rolePermissionMapper.getAllRoles());
      }
      return all;
   }

   public void addRole(Role role) {
      roles.put(role.getName(), role);
   }

   public boolean hasRole(String name) {
      return roles.containsKey(name) || (rolePermissionMapper != null && rolePermissionMapper.hasRole(name));
   }

   public Role getRole(String name) {
      Role role = roles.get(name);
      if (role != null) {
         return role;
      } else if (rolePermissionMapper != null) {
         return rolePermissionMapper.getRole(name);
      } else {
         return null;
      }
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GlobalAuthorizationConfiguration that = (GlobalAuthorizationConfiguration) o;
      return Objects.equals(roleMapperConfiguration, that.roleMapperConfiguration) &&
            Objects.equals(permissionMapperConfiguration, that.permissionMapperConfiguration) &&
            Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return Objects.hash(roleMapperConfiguration, permissionMapperConfiguration, attributes);
   }

   @Override
   public String toString() {
      return "GlobalAuthorizationConfiguration{" +
            "roleMapperConfiguration=" + roleMapperConfiguration +
            "permissionMapperConfiguration=" + permissionMapperConfiguration +
            ", attributes=" + attributes +
            '}';
   }
}
