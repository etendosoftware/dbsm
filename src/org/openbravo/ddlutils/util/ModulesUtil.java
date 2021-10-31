package org.openbravo.ddlutils.util;

import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.ddlutils.coreutils.CoreMetadata;
import org.openbravo.ddlutils.coreutils.JarCoreMetadata;
import org.openbravo.ddlutils.coreutils.SourceCoreMetadata;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Vector;

public class ModulesUtil {

    public static Boolean coreInSources   = true;
    public static final String MODULES_JAR  = "build/etendo/modules";
    public static final String MODULES_BASE = "modules";
    public static final String MODULES_CORE = "modules_core";
    public static String[] moduleDirs = new String[] {MODULES_BASE, MODULES_CORE};

    static {
        checkCoreInSources(coreInSources());
    }

    public static Vector<File> get(String path) {

        checkCoreInSources(coreInSources());

        String auxPath = path;
        final String rootWorkDir = getProjectRootDir();

        /**
         * The core is in JAR.
         * Set the 'path' to the root project to take into account all the modules folders:
         * - root/modules
         * - root/build/etendo/modules
         */
        if (!ModulesUtil.coreInSources) {
            auxPath = rootWorkDir;
        }

        Vector<File> dirs = new Vector<File>();
        String finalAuxPath = auxPath;
        Arrays.asList(moduleDirs).forEach(dirName -> {
            File modules = new File(finalAuxPath, dirName);
            if(modules.isDirectory() && modules.listFiles() != null) {
                dirs.addAll(Arrays.asList(Objects.requireNonNull(modules.listFiles())));
            }
        });
        return dirs;
    }

    public static void checkCoreInSources(Boolean isCoreInSources) {
        coreInSources = isCoreInSources;
        CoreMetadata coreData = null;
        String workDir = getProjectRootDir();

        if (isCoreInSources) {
            coreData = new SourceCoreMetadata(workDir);
        } else {
            coreData = new JarCoreMetadata(workDir);
        }

        moduleDirs = coreData.getAllModulesLocation();
    }

    /**
     * Verifies that the core is in sources
     * TODO: See if there is a more secure solution.
     * @return
     */
    public static Boolean coreInSources() {
        String rootWorkDir = getProjectRootDir();
        File rootLocation = new File(rootWorkDir);

        // File used to verify that the core is in sources
        File source = new File(rootLocation, "modules_core");

        return  (source.exists() && source.isDirectory());
    }

    /**
     * Obtains the 'source.path' from the Openbravo.properties file
     * @return The root dir of the current project
     */
    public static String getProjectRootDir() {
        return (String) OBPropertiesProvider.getInstance()
                .getOpenbravoProperties()
                .get("source.path");
    }

    public static File[] union(File[] array1, File[] array2) {
        File[] ret = new File[array1.length + array2.length];
        int i = 0;
        for (int j = 0; j < array1.length; j++) {
            ret[i] = array1[j];
            i ++;
        }
        for (int k = 0; k < array2.length; k++) {
            ret[i] = array2[k];
            i++;
        }
        return ret;
    }
}
