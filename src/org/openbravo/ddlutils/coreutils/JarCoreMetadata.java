package org.openbravo.ddlutils.coreutils;

import org.openbravo.ddlutils.util.ModulesUtil;

import java.util.List;

public class JarCoreMetadata extends CoreMetadata {

    public JarCoreMetadata(String rootProject) {
        super(rootProject);
    }

    public JarCoreMetadata(String rootProject, List<String> extraModules) {
        super(rootProject, extraModules);
    }

    @Override
    public void loadRequiredModules() {
        this.requiredModulesLocation.add(ModulesUtil.MODULES_JAR);
    }

    @Override
    public void loadOptionalModules() {
        this.optionalModulesLocation.add(ModulesUtil.MODULES_BASE);
    }
}
