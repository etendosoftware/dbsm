/*
 ************************************************************************************
 * Copyright (C) 2001-2016 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.model;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.ddlutils.translation.NullTranslation;
import org.apache.ddlutils.translation.Translation;
import org.apache.ddlutils.util.Jdbc3Utils;

/**
 * 
 * @author adrian
 */
public abstract class ValueObject implements Cloneable {

  /** The JDBC type code, one of the constants in {@link java.sql.Types}. */
  protected int _typeCode;
  /** The default value. */
  protected String _defaultValue;
  protected Translation _translation = new NullTranslation();

  public ValueObject() {
    _typeCode = Types.VARCHAR;
    _defaultValue = null;
  }

  /**
   * Returns the code (one of the constants in {@link java.sql.Types}) of the JDBC type of the
   * column.
   * 
   * @return The type code
   */
  public int getTypeCode() {
    return _typeCode;
  }

  /**
   * Sets the code (one of the constants in {@link java.sql.Types}) of the JDBC type of the column.
   * 
   * @param typeCode
   *          The type code
   */
  public void setTypeCode(int typeCode) {
    String _type = TypeMap.getJdbcTypeName(typeCode);
    if (_type == null) {
      throw new ModelException("Unknown JDBC type code " + typeCode);
    }
    _typeCode = typeCode;
  }

  /**
   * Returns the JDBC type of the parameter.
   * 
   * @return The type
   */
  public String getType() {
    return TypeMap.getJdbcTypeName(_typeCode);
  }

  /**
   * Sets the JDBC type of the parameter.
   * 
   * @param type
   *          The type
   */
  public void setType(String type) {
    Integer typeCode = TypeMap.getJdbcTypeCode(type);

    if (typeCode == null) {
      throw new ModelException("Unknown JDBC type " + type);
    } else {
      _typeCode = typeCode.intValue();
    }
  }

  /**
   * Returns the default value of the column.
   * 
   * @return The default value
   */
  public String getDefaultValue() {
    return _defaultValue;
  }

  /**
   * Returns the default value of the column.
   * 
   * @return The default value
   */
  public String getDefaultValueAttr() {
    return null;
  }

  /**
   * Tries to parse the default value of the column and returns it as an object of the corresponding
   * java type. If the value could not be parsed, then the original definition is returned.
   * 
   * @return The parsed default value
   */
  public Object getParsedDefaultValue() {
    if ((_defaultValue != null) && (_defaultValue.length() > 0) && !isDefaultFunction()) {
      try {
        switch (_typeCode) {
        case Types.TINYINT:
        case Types.SMALLINT:
          return new Short(_defaultValue);
        case Types.INTEGER:
          return new Integer(_defaultValue);
        case Types.BIGINT:
          return new Long(_defaultValue);
        case Types.DECIMAL:
        case Types.NUMERIC:
          return new BigDecimal(_defaultValue);
        case Types.REAL:
          return new Float(_defaultValue);
        case Types.DOUBLE:
        case Types.FLOAT:
          return new Double(_defaultValue);
        case Types.DATE:
          return Date.valueOf(_defaultValue);
        case Types.TIME:
          return Time.valueOf(_defaultValue);
        case Types.TIMESTAMP:
          return Timestamp.valueOf(_defaultValue);
        case Types.BIT:
          return ConvertUtils.convert(_defaultValue, Boolean.class);
        default:
          if (Jdbc3Utils.supportsJava14JdbcTypes()
              && (_typeCode == Jdbc3Utils.determineBooleanTypeCode())) {
            return ConvertUtils.convert(_defaultValue, Boolean.class);
          }
          break;
        }
      } catch (NumberFormatException ex) {
        return null;
      } catch (IllegalArgumentException ex) {
        return null;
      }
    }
    return _defaultValue;
  }

  /**
   * Sets the default value of the column. Note that this expression will be used within quotation
   * marks when generating the column, and thus is subject to the conversion rules of the target
   * database.
   * 
   * @param defaultValue
   *          The default value
   */
  public void setDefaultValue(String defaultValue) {
    _defaultValue = defaultValue;
  }

  /**
   * Sets the default value of the column. Note that this expression will be used within quotation
   * marks when generating the column, and thus is subject to the conversion rules of the target
   * database.
   * 
   * @param defaultValue
   *          The default value
   */
  public void setDefaultValueAttr(String defaultValue) {
    if (_defaultValue == null) {
      _defaultValue = defaultValue;
    }
  }

  /**
   * Check if the default value is a function
   * 
   */
  public boolean isDefaultFunction() {

    if (_defaultValue == null) {
      return false;
    } else if ("NULL".equals(_defaultValue)) {
      return true;
    } else {
      switch (_typeCode) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.DECIMAL:
      case Types.NUMERIC:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.FLOAT:
        return false;
      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        return "SYSDATE".equals(_defaultValue.toUpperCase())
            || "NOW()".equals(_defaultValue.toUpperCase());
      case Types.BIT:
      default:
        return false;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    ValueObject result = (ValueObject) super.clone();

    result._typeCode = _typeCode;
    result._defaultValue = _defaultValue;

    return result;
  }

  public void setTranslation(Translation translation) {
    _translation = translation;
  }
}
