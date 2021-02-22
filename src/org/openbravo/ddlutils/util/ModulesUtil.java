package org.openbravo.ddlutils.util;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.Vector;

public class ModulesUtil {
    public static final String MODULES_BASE = "modules";
    public static final String MODULES_CORE = "modules_core";
    public static String[] moduleDirs = new String[] {MODULES_BASE, MODULES_CORE};
    public static Vector<File> get(String path) {
        Vector<File> dirs = new Vector<File>();
        Arrays.asList(moduleDirs).forEach(dirName -> {
            File modules = new File(path, dirName);
            if(modules.isDirectory() && modules.listFiles() != null) {
                dirs.addAll(Arrays.asList(Objects.requireNonNull(modules.listFiles())));
            }
        });
        return dirs;
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
