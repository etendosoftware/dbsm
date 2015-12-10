/*
 ************************************************************************************
 * Copyright (C) 2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.dbsm.test.model;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Function;
import org.apache.ddlutils.model.Sequence;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Trigger;
import org.apache.ddlutils.model.View;
import org.apache.ddlutils.platform.ExcludeFilter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openbravo.dbsm.test.base.DbsmTest;

/**
 * Test cases to test the function based indexes support
 * 
 * @author AugustoMauch
 *
 */
@RunWith(Parameterized.class)
public class CheckExcludeFilter extends DbsmTest {

  private static final String EXPORT_DIR = "/tmp/export-test";

  public CheckExcludeFilter(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  @Test
  // Tests that tables in the exclude filter are not exported
  public void tableIsExcluded() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_TABLE.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeTable.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/tables/NOT_EXCLUDED_TABLE.xml");
    File excludedFile = new File(EXPORT_DIR + "/tables/EXCLUDED_TABLE.xml");
    assertThat(excludedTableExistsInDb("EXCLUDED_TABLE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that tables in the exclude filter are not exported
  public void viewIsExcluded() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_VIEW.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeView.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/views/NOT_EXCLUDED_VIEW.xml");
    File excludedFile = new File(EXPORT_DIR + "/views/EXCLUDED_VIEW.xml");
    assertThat(excludedViewExistsInDb("EXCLUDED_VIEW"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that tables in the exclude filter are not exported
  public void triggerIsExcluded() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_TRIGGER.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeTrigger.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/triggers/NOT_EXCLUDED_TRIGGER.xml");
    File excludedFile = new File(EXPORT_DIR + "/triggers/EXCLUDED_TRIGGER.xml");
    assertThat(excludedTriggerExistsInDb("EXCLUDED_TRIGGER"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that tables in the exclude filter are not exported
  public void functionIsExcluded() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_FUNCTION.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeFunction.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/functions/NOT_EXCLUDED_FUNCTION.xml");
    File excludedFile = new File(EXPORT_DIR + "/functions/EXCLUDED_FUNCTION.xml");
    assertThat(excludedFunctionExistsInDb("EXCLUDED_FUNCTION"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that tables in the exclude filter are not exported
  public void sequenceIsExcluded() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEQUENCE.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeSequence.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/sequences/NOT_EXCLUDED_SEQUENCE.xml");
    File excludedFile = new File(EXPORT_DIR + "/sequences/EXCLUDED_SEQUENCE.xml");
    assertThat(excludedSequenceExistsInDb("EXCLUDED_SEQUENCE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  private boolean excludedFunctionExistsInDb(String functionName) {
    Platform platform = getPlatform();
    Database database = platform.loadModelFromDatabase(new ExcludeFilter());
    Function function = database.findFunction(functionName);
    return (function != null);
  }

  private boolean excludedTableExistsInDb(String tableName) {
    Platform platform = getPlatform();
    Database database = platform.loadModelFromDatabase(new ExcludeFilter());
    Table table = database.findTable(tableName);
    return (table != null);
  }

  private boolean excludedViewExistsInDb(String viewName) {
    Platform platform = getPlatform();
    Database database = platform.loadModelFromDatabase(new ExcludeFilter());
    View view = database.findView(viewName);
    return (view != null);
  }

  private boolean excludedTriggerExistsInDb(String triggerName) {
    Platform platform = getPlatform();
    Database database = platform.loadModelFromDatabase(new ExcludeFilter());
    Trigger trigger = database.findTrigger(triggerName);
    return (trigger != null);
  }

  private boolean excludedSequenceExistsInDb(String sequenceName) {
    Platform platform = getPlatform();
    Database database = platform.loadModelFromDatabase(new ExcludeFilter());
    Sequence sequence = database.findSequence(sequenceName);
    return (sequence != null);
  }
}
