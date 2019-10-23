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
import org.apache.ddlutils.model.MaterializedView;
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
  // Tests that tables defined in the exclude filter using wildcards are not exported
  public void tableIsExcludedWithWildCard() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_TABLE.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeTableWithWildcard.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/tables/NOT_EXCLUDED_TABLE.xml");
    File excludedFile = new File(EXPORT_DIR + "/tables/EXCLUDED_TABLE.xml");
    assertThat(excludedTableExistsInDb("EXCLUDED_TABLE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded tables are not exported having several wildcards defined in the exclude
  // filter
  public void tableIsExcludedWithMultipleWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_TABLES.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeTableWithMultipleWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/tables/NOT_EXCLUDED_TABLE.xml");
    File excludedFile = new File(EXPORT_DIR + "/tables/EXCLUDED_TABLE.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/tables/ALSO_EXCLUDED_TABLE.xml");
    assertThat(excludedTableExistsInDb("EXCLUDED_TABLE"), equalTo(true));
    assertThat(excludedTableExistsInDb("ALSO_EXCLUDED_TABLE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded tables are not exported mixing wildcards and non wildcards in the exclude
  // filter
  public void tableIsExcludedWithWildcardsAndNonWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_TABLES.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeTableWithWildcardsAndNonWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/tables/NOT_EXCLUDED_TABLE.xml");
    File excludedFile = new File(EXPORT_DIR + "/tables/EXCLUDED_TABLE.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/tables/ALSO_EXCLUDED_TABLE.xml");
    assertThat(excludedTableExistsInDb("EXCLUDED_TABLE"), equalTo(true));
    assertThat(excludedTableExistsInDb("ALSO_EXCLUDED_TABLE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that views in the exclude filter are not exported
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
  // Tests that materialized views in the exclude filter are not exported
  public void materializedViewIsExcluded() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_MATERIALIZEDVIEW.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeMaterializedView.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(
        EXPORT_DIR + "/materializedViews/NOT_EXCLUDED_MATERIALIZEDVIEW.xml");
    File excludedFile = new File(EXPORT_DIR + "/materializedViews/EXCLUDED_MATERIALIZEDVIEW.xml");
    assertThat(excludedMaterializedViewExistsInDb("EXCLUDED_MATERIALIZEDVIEW"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that views defined in the exclude filter using wildcards are not exported
  public void viewIsExcludedWithWildCard() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_VIEW.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeViewWithWildcard.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/views/NOT_EXCLUDED_VIEW.xml");
    File excludedFile = new File(EXPORT_DIR + "/views/EXCLUDED_VIEW.xml");
    assertThat(excludedViewExistsInDb("EXCLUDED_VIEW"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded views are not exported having several wildcards defined in the exclude
  // filter
  public void viewIsExcludedWithMultipleWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_VIEWS.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeViewWithMultipleWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/views/NOT_EXCLUDED_VIEW.xml");
    File excludedFile = new File(EXPORT_DIR + "/views/EXCLUDED_VIEW.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/views/ALSO_EXCLUDED_VIEW.xml");
    assertThat(excludedViewExistsInDb("EXCLUDED_VIEW"), equalTo(true));
    assertThat(excludedViewExistsInDb("ALSO_EXCLUDED_VIEW"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded views are not exported mixing wildcards and non wildcards in the exclude
  // filter
  public void viewIsExcludedWithWildcardsAndNonWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_VIEWS.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeViewWithWildcardsAndNonWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/views/NOT_EXCLUDED_VIEW.xml");
    File excludedFile = new File(EXPORT_DIR + "/views/EXCLUDED_VIEW.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/views/ALSO_EXCLUDED_VIEW.xml");
    assertThat(excludedViewExistsInDb("EXCLUDED_VIEW"), equalTo(true));
    assertThat(excludedViewExistsInDb("ALSO_EXCLUDED_VIEW"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that triggers in the exclude filter are not exported
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
  // Tests that triggers defined in the exclude filter using wildcards are not exported
  public void triggerIsExcludedWithWildCard() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_TRIGGER.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeTriggerWithWildcard.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/triggers/NOT_EXCLUDED_TRIGGER.xml");
    File excludedFile = new File(EXPORT_DIR + "/triggers/EXCLUDED_TRIGGER.xml");
    assertThat(excludedTriggerExistsInDb("EXCLUDED_TRIGGER"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded triggers are not exported having several wildcards defined in the exclude
  // filter
  public void triggerIsExcludedWithMultipleWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_TRIGGERS.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeTriggerWithMultipleWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/triggers/NOT_EXCLUDED_TRIGGER.xml");
    File excludedFile = new File(EXPORT_DIR + "/triggers/EXCLUDED_TRIGGER.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/triggers/ALSO_EXCLUDED_TRIGGER.xml");
    assertThat(excludedTriggerExistsInDb("EXCLUDED_TRIGGER"), equalTo(true));
    assertThat(excludedTriggerExistsInDb("ALSO_EXCLUDED_TRIGGER"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded triggers are not exported mixing wildcards and non wildcards in the exclude
  // filter
  public void triggerIsExcludedWithWildcardsAndNonWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_TRIGGERS.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(
        new File("model/excludeFilter/excludeTriggerWithWildcardsAndNonWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/triggers/NOT_EXCLUDED_TRIGGER.xml");
    File excludedFile = new File(EXPORT_DIR + "/triggers/EXCLUDED_TRIGGER.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/triggers/ALSO_EXCLUDED_TRIGGER.xml");
    assertThat(excludedTriggerExistsInDb("EXCLUDED_TRIGGER"), equalTo(true));
    assertThat(excludedTriggerExistsInDb("ALSO_EXCLUDED_TRIGGER"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that functions in the exclude filter are not exported
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
  // Tests that functions defined in the exclude filter using wildcards are not exported
  public void functionIsExcludedWithWildCard() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_FUNCTION.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeFunctionWithWildcard.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/functions/NOT_EXCLUDED_FUNCTION.xml");
    File excludedFile = new File(EXPORT_DIR + "/functions/EXCLUDED_FUNCTION.xml");
    assertThat(excludedFunctionExistsInDb("EXCLUDED_FUNCTION"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded functions are not exported having several wildcards defined in the exclude
  // filter
  public void functionIsExcludedWithMultipleWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_FUNCTIONS.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeFunctionWithMultipleWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/functions/NOT_EXCLUDED_FUNCTION.xml");
    File excludedFile = new File(EXPORT_DIR + "/functions/EXCLUDED_FUNCTION.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/functions/ALSO_EXCLUDED_FUNCTION.xml");
    assertThat(excludedFunctionExistsInDb("EXCLUDED_FUNCTION"), equalTo(true));
    assertThat(excludedFunctionExistsInDb("ALSO_EXCLUDED_FUNCTION"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded functions are not exported mixing wildcards and non wildcards in the
  // exclude filter
  public void functionIsExcludedWithWildcardsAndNonWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_FUNCTIONS.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(
        new File("model/excludeFilter/excludeFunctionWithWildcardsAndNonWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/functions/NOT_EXCLUDED_FUNCTION.xml");
    File excludedFile = new File(EXPORT_DIR + "/functions/EXCLUDED_FUNCTION.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/functions/ALSO_EXCLUDED_FUNCTION.xml");
    assertThat(excludedFunctionExistsInDb("EXCLUDED_FUNCTION"), equalTo(true));
    assertThat(excludedFunctionExistsInDb("ALSO_EXCLUDED_FUNCTION"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that sequences in the exclude filter are not exported
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

  @Test
  // Tests that sequences defined in the exclude filter using wildcards are not exported
  public void sequenceIsExcludedWithWildCard() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEQUENCE.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(new File("model/excludeFilter/excludeSequenceWithWildcard.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/sequences/NOT_EXCLUDED_SEQUENCE.xml");
    File excludedFile = new File(EXPORT_DIR + "/sequences/EXCLUDED_SEQUENCE.xml");
    assertThat(excludedSequenceExistsInDb("EXCLUDED_SEQUENCE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded sequences are not exported having several wildcards defined in the exclude
  // filter
  public void sequenceIsExcludedWithMultipleWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_SEQUENCES.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter
        .fillFromFile(new File("model/excludeFilter/excludeSequenceWithMultipleWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/sequences/NOT_EXCLUDED_SEQUENCE.xml");
    File excludedFile = new File(EXPORT_DIR + "/sequences/EXCLUDED_SEQUENCE.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/sequences/ALSO_EXCLUDED_SEQUENCE.xml");
    assertThat(excludedSequenceExistsInDb("EXCLUDED_SEQUENCE"), equalTo(true));
    assertThat(excludedSequenceExistsInDb("ALSO_EXCLUDED_SEQUENCE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
  }

  @Test
  // Tests that excluded sequences are not exported mixing wildcards and non wildcards in the
  // exclude filter
  public void sequenceIsExcludedWithWildcardsAndNonWildcards() throws IOException {
    resetDB();
    updateDatabase("excludeFilter/BASE_MODEL_WITH_SEVERAL_EXCLUDED_SEQUENCES.xml");
    ExcludeFilter excludeFilter = new ExcludeFilter();
    excludeFilter.fillFromFile(
        new File("model/excludeFilter/excludeSequenceWithWildcardsAndNonWildcards.xml"));
    setExcludeFilter(excludeFilter);
    exportDatabase(EXPORT_DIR);
    File notExcludedFile = new File(EXPORT_DIR + "/sequences/NOT_EXCLUDED_SEQUENCE.xml");
    File excludedFile = new File(EXPORT_DIR + "/sequences/EXCLUDED_SEQUENCE.xml");
    File alsoExcludedFile = new File(EXPORT_DIR + "/functions/ALSO_EXCLUDED_SEQUENCE.xml");
    assertThat(excludedSequenceExistsInDb("EXCLUDED_SEQUENCE"), equalTo(true));
    assertThat(excludedSequenceExistsInDb("ALSO_EXCLUDED_SEQUENCE"), equalTo(true));
    assertThat(notExcludedFile.exists(), equalTo(true));
    assertThat(excludedFile.exists(), equalTo(false));
    assertThat(alsoExcludedFile.exists(), equalTo(false));
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

  private boolean excludedMaterializedViewExistsInDb(String materializedViewName) {
    Platform platform = getPlatform();
    Database database = platform.loadModelFromDatabase(new ExcludeFilter());
    MaterializedView materializedView = database.findMaterializedView(materializedViewName);
    return (materializedView != null);
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
