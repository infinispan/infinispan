package org.infinispan.client.rest.configuration;

import java.util.Properties;

import javax.security.auth.Subject;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public record AuthenticationConfiguration(Subject clientSubject, boolean enabled, String mechanism, String realm,
                                          String username, char[] password, Properties properties) {
}
