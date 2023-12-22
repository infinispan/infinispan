package org.infinispan.security;

/**
 * PrincipalRoleMapperContext.
 *
 * @author Tristan Tarrant
 * @since 7.0
 * @deprecated since 14.0. To be removed in 17.0. Use {@link AuthorizationMapperContext} instead
 */
@Deprecated(forRemoval=true, since = "14.0")
public interface PrincipalRoleMapperContext extends AuthorizationMapperContext {
}
