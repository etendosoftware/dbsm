/*
 ************************************************************************************
 * Copyright (C) 2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.DatabaseOperationException;
import org.apache.ddlutils.Platform;

/** Writes statements in {@code out File} */
public class FileBatchEvaluator implements SQLBatchEvaluator {
  private static final Log log = LogFactory.getLog(PlatformImplBase.class);
  private Path outPath;

  public FileBatchEvaluator(File out) {
    outPath = out.toPath();
    try {
      Files.deleteIfExists(outPath);
      Files.createFile(outPath);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't create file " + out, e);
    }

  }

  @Override
  public void setPlatform(Platform p) {
    // platform not needed
  }

  @Override
  public int evaluateBatch(Connection connection, List<String> statements, boolean continueOnError,
      long firstSqlCommandIndex) throws DatabaseOperationException {
    for (String sql : statements) {
      try {
        Files.write(outPath, sql.getBytes(), StandardOpenOption.APPEND);
      } catch (IOException e) {
        log.error("Error writting to " + outPath, e);
      }
    }
    return 0; // 0 failures
  }

  @Override
  public int evaluateBatchRealBatch(Connection connection, List<String> sql,
      boolean continueOnError) throws DatabaseOperationException {
    return evaluateBatch(connection, sql, continueOnError, 0);
  }

  @Override
  public void setLogInfoSucessCommands(boolean logInfoSucessCommands) {

  }

  @Override
  public boolean isLogInfoSucessCommands() {
    return false;
  }

  @Override
  public boolean isDBEvaluator() {
    return false;
  }
}
