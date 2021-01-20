package org.infinispan.configuration.global;

public interface GlobalRolesConfigurationChildBuilder {
   GlobalRoleConfigurationBuilder role(String name);

   GlobalRoleConfigurationBuilder inheritable(boolean inheritable);
}
