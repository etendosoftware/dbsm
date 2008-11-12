package org.openbravo.test.dbsm;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.ddlutils.task.VerbosityLevel;
import org.openbravo.base.exception.OBException;
import org.openbravo.ddlutils.task.ExportDataXMLMod;

public class TestExportData extends TestCase {
	public void testConfig() {
		ExportDataXMLMod ecs = new ExportDataXMLMod();
		Properties p = new Properties();
		try {
			p.load(this.getClass().getResourceAsStream("Openbravo.properties"));
		}
		catch (Exception e) {
			throw new OBException(e);
		}
		ecs.setDriver(p.getProperty("bbdd.driver"));
		ecs.setUrl(p.getProperty("bbdd.url"));
		ecs.setUser(p.getProperty("bbdd.user"));
		ecs.setFilter("com.openbravo.db.OpenbravoMetadataFilter");
		ecs.setPassword(p.getProperty("bbdd.password"));
		ecs.setExcludeobjects("com.openbravo.db.OpenbravoExcludeFilter");
		ecs.setModuledir(new File("/home/openbravo/workspaceModularity/openbravo/modules/"));
		ecs.setModel(new File("/home/openbravo/workspaceModularity/openbravo/src-db/database/model"));
		ecs.setOutput(new File("/home/openbravo/workspaceModularity/openbravo/src-db/database/sourcedata/"));
		// ecs.setCoreData("/home/openbravo/workspaceModularity/openbravo/src-db/database/sourcedata");
		// ecs.setIndustryTemplate("test-db1");
		ecs.setUserId("0");
		ecs.setVerbosity(new VerbosityLevel(p.getProperty("bbdd.verbosity")));
		ecs.setPropertiesFile("/home/openbravo/workspaceModularity/openbravo/config/Openbravo.properties");
		/*
		 * driver="${bbdd.driver}" url="${bbdd.owner.url}" user="${bbdd.user}"
		 * filter="com.openbravo.db.OpenbravoMetadataFilter"
		 * password="${bbdd.password}"
		 * excludeobjects="com.openbravo.db.OpenbravoExcludeFilter"
		 * moduledir="${base.modules}" model="model"
		 * coreData="${base.db}/sourcedata" verbosity="${bbdd.verbosity}"
		 * industryTemplate="${industryTemplate}" userId="0"
		 * propertiesFile="${base.config}/Openbravo.properties" />
		 */
		ecs.execute();
	}
}
