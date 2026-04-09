/*
 ************************************************************************************
 * Copyright (C) 2001-2020 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.util;

/**
 * Exception specific to errors related to DB Source Manager (DBSM).
 * <p>
 * This exception extends {@link RuntimeException}, making it an unchecked exception
 * that does not need to be declared in method signatures.
 * </p>
 *
 * <p>
 * It is used to wrap errors that occur during DB Source Manager operations,
 * such as schema generation, validation, or script execution.
 * </p>
 */
public class DBSMException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Constructs a new {@code DBSMException} with the specified detail message.
   *
   * @param message the detail message describing the error
   */
  public DBSMException(String message) {
    super(message);
  }

  /**
   * Constructs a new {@code DBSMException} with the specified detail message and cause.
   *
   * @param message the detail message describing the error
   * @param cause the original cause of the exception
   */
  public DBSMException(String message, Throwable cause) {
    super(message, cause);
  }
}
