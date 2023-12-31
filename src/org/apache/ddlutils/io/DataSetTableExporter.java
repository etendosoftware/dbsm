/*
 ************************************************************************************
 * Copyright (C) 2016 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.io;

import java.io.OutputStream;
import java.util.Map;

import org.apache.ddlutils.model.Database;
import org.openbravo.ddlutils.util.OBDatasetTable;

/**
 * Classes implementing this interface will be used by the ExportSampledata class to export a
 * dataset table to an output stream
 *
 */
public interface DataSetTableExporter {
  /**
   * Exports a dataset table to an output stream
   * 
   * @return true if any records have been exported, false otherwise
   */
  public boolean exportDataSet(Database model, OBDatasetTable dsTable, OutputStream output,
      String moduleId, Map<String, Object> customParams, boolean orderByTableId);
}
