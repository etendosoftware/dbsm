package org.openbravo.ddlutils.coreutils;

import org.openbravo.ddlutils.util.ModulesUtil;

import java.util.List;

public class SourceCoreMetadata extends CoreMetadata {

    public SourceCoreMetadata(String rootProject) {
        super(rootProject);
    }

    public SourceCoreMetadata(String rootProject, List<String> extraModules) {
        super(rootProject, extraModules);
    }

    @Override
    public void loadRequiredModules() {
        this.requiredModulesLocation.add(ModulesUtil.MODULES_BASE);
        this.requiredModulesLocation.add(ModulesUtil.MODULES_CORE);
    }

    @Override
    public void loadOptionalModules() {
        this.optionalModulesLocation.add(ModulesUtil.MODULES_JAR);
    }

}
