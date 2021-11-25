package org.openbravo.ddlutils.coreutils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class CoreMetadata {

    List<String> requiredModulesLocation;
    List<String> optionalModulesLocation;
    List<String> extraModulesLocation;

    String rootProject;

    public CoreMetadata(String rootProject) {
        this.requiredModulesLocation = new ArrayList<>();
        this.optionalModulesLocation = new ArrayList<>();
        this.extraModulesLocation    = new ArrayList<>();

        this.rootProject = rootProject;
        loadRequiredModules();
        loadOptionalModules();
    }

    public CoreMetadata(String rootProject, List<String> extraModules) {
        this(rootProject);
        this.extraModulesLocation.addAll(extraModules);
    }

    public abstract void loadRequiredModules();
    public abstract void loadOptionalModules();

    public String[] getAllModulesLocation() {
        List<String> modules = new ArrayList<>();
        modules.addAll(getValidModules(requiredModulesLocation, true));
        modules.addAll(getValidModules(optionalModulesLocation, false));
        modules.addAll(getValidModules(extraModulesLocation, false));
        return modules.toArray(new String[0]);
    }

    public List<String> getValidModules(List<String> modules, Boolean required) {
        List<String> moduleList = new ArrayList<>();
        for (String module : modules) {
            File moduleLocation = new File(rootProject + File.separator + module);

            if (moduleLocation.exists() && moduleLocation.isDirectory()) {
                moduleList.add(module);
            } else if (required) {
                throw new IllegalArgumentException("The required module '" + module + "' directory " +
                        "does not exists.\nModule path location: " + moduleLocation.getAbsolutePath());
            }
        }
        return moduleList;
    }
}
