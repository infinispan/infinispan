package org.infinispan.cli.impl;

import java.io.File;
import java.io.IOException;

import org.aesh.command.registry.CommandRegistry;
import org.aesh.readline.alias.AliasManager;

public class CliAliasManager extends AliasManager {

    private final CommandRegistry registry;

    public CliAliasManager(File aliasFile, boolean persistAlias,
                           CommandRegistry registry) throws IOException {
           super(aliasFile, persistAlias);
           this.registry = registry;
    }

    @Override
    public boolean verifyNoNewAliasConflict(String aliasName) {
        if(registry != null && registry.contains(aliasName))
            return false;
        else
            return true;
    }


}
