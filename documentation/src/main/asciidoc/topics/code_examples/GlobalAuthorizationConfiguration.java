GlobalRoleConfigurationBuilder global = new GlobalConfigurationBuilder();
  global.security().authorization().enable()
          .principalRoleMapper(new ClusterRoleMapper())
          .role("admin").permission(AuthorizationPermission.ADMIN)
          .role("reader").permission(AuthorizationPermission.ALL_READ);
