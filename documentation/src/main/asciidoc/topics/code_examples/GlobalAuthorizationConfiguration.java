GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
  global.security().authorization().enable()
          .principalRoleMapper(new ClusterRoleMapper())
          .role("myroleone").permission(AuthorizationPermission.ALL_WRITE)
          .role("myroletwo").permission(AuthorizationPermission.ALL_READ);
