MutablePermissionMapper mapper = (MutablePermissionMapper) cacheManager.getCacheManagerConfiguration().security().authorization().rolePermissionMapper();
mapper.addRole(Role.newRole("myroleone", true, AuthorizationPermission.ALL_WRITE, AuthorizationPermission.LISTEN));
mapper.addRole(Role.newRole("myroletwo", true, AuthorizationPermission.READ, AuthorizationPermission.WRITE));
