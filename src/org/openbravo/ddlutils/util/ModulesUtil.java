package org.openbravo.ddlutils.util;

import com.etendoerp.properties.EtendoPropertiesProvider;
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
        try {
            checkCoreInSources(coreInSources());
        } catch (Exception e) {
            System.out.println("Error initializing static block in dbsm ModulesUtil class." + e.getMessage());
        }
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

    public static void checkCoreInSources(Boolean isCoreInSources, String rootDir) {
        coreInSources = isCoreInSources;
        CoreMetadata coreData = null;
        String workDir = rootDir;
        if (rootDir == null || rootDir.isBlank() || rootDir.isEmpty()) {
            workDir = getProjectRootDir();
        }

        if (isCoreInSources) {
            coreData = new SourceCoreMetadata(workDir);
        } else {
            coreData = new JarCoreMetadata(workDir);
        }

        moduleDirs = coreData.getAllModulesLocation();
    }

    public static void checkCoreInSources(Boolean isCoreInSources) {
        checkCoreInSources(isCoreInSources,"");
    }

    /**
     * Verifies that the core is in sources
     * TODO: See if there is a more secure solution.
     * @return
     */
    public static Boolean coreInSources() {
        return coreInSources("");
    }

    public static Boolean coreInSources(String rootDir) {
        String rootWorkDir = rootDir;
        if (rootDir == null || rootDir.isBlank() || rootDir.isEmpty()) {
            rootWorkDir = getProjectRootDir();
        }
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
        String sourcePath = (String) EtendoPropertiesProvider.getInstance()
                            .getEtendoProperties()
                            .get("source.path");
        if (sourcePath == null || sourcePath.isBlank() || sourcePath.isEmpty()) {
            throw new IllegalArgumentException("The property 'source.path' is not defined.");
        }
        return sourcePath;
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
