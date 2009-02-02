/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.platform.postgresql;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author adrian
 */
public class RuleProcessor {

  private ArrayList<ViewField> _viewfields = new ArrayList<ViewField>();
  private String _viewtable = null;
  private boolean _bupdatable = false;

  /** Creates a new instance of RuleProcessor */
  public RuleProcessor(String sql) {

    String field = "\\s*((.+?)(\\s+?[Aa][Ss]\\s+?(.+?))??)\\s*?((,)|(\\s[Ff][Rr][Oo][Mm]\\s+))";
    Pattern p = Pattern.compile("^\\s*[Ss][Ee][Ll][Ee][Cc][Tt]\\s" + field); //  

    Pattern pField = Pattern.compile("\\w+(\\.\\w+)?");
    Pattern pFieldas = Pattern.compile("\\w+");
    Pattern pTable = Pattern.compile("\\w+(\\s+\\w+)?");

    Matcher m = p.matcher(sql);
    if (m.find()) {
      addField(removeNameOfTable(m.group(2)), m.group(4));
      String sseparator = m.group(5);
      int offset = m.end();

      p = Pattern.compile(field);
      m = p.matcher(sql);

      while (",".equals(sseparator)) {
        if (m.find(offset)) {
          if (pField.matcher(m.group(2)).matches()
              && (m.group(4) == null || pFieldas.matcher(m.group(4)).matches())) {
            addField(removeNameOfTable(m.group(2)), m.group(4));
            sseparator = m.group(5);
            offset = m.end();
          } else {
            // Field definition does not matches
            _bupdatable = false;
            return;
          }
        } else {
          // No field declaration after ","
          _bupdatable = false;
          return;
        }
      }
      // From reached

      p = Pattern.compile("(.+?)\\s+?[Ww][Hh][Ee][Rr][Ee]\\s");
      m = p.matcher(sql);
      if (m.find(offset)) {
        if (pTable.matcher(m.group(1)).matches()) {
          addTable(m.group(1));
        } else {
          // table definition does not matches
          _bupdatable = false;
          return;
        }
      } else {
        // No table name found
        _bupdatable = false;
        return;
      }

    } else {
      // No select command found
      _bupdatable = false;
      return;
    }

    _bupdatable = true;
  }

  private String removeNameOfTable(String field) {
    return field.substring(field.indexOf(".") + 1); // if field not contains
    // '.' indexof returns
    // '-1', in this case
    // '-1 +1 = 0' and the
    // function will return
    // the entire string
  }

  public boolean isUpdatable() {
    return _bupdatable;
  }

  private void addField(String field, String fieldas) {
    _viewfields.add(new ViewField(field, fieldas));
  }

  private void addTable(String table) {
    _viewtable = table;
  }

  public String getViewTable() {
    return _viewtable;
  }

  public ArrayList<ViewField> getViewFields() {
    return _viewfields;
  }

  public static class ViewField {

    private String _field;
    private String _fieldas;

    public ViewField(String field, String fieldas) {
      _field = field;
      _fieldas = fieldas;
    }

    public String getField() {
      return _field;
    }

    public String getFieldas() {
      return _fieldas == null ? _field : _fieldas;
    }
  }

  public String toString() {

    StringBuffer result = new StringBuffer();
    result.append("RuleProcessor [viewTable=");
    result.append(_viewtable);
    result.append("; viewFields = (");
    for (ViewField f : _viewfields) {
      result.append(f.getField());
      result.append(" AS ");
      result.append(f.getFieldas());
      result.append(",");
    }
    result.append(");]");

    return result.toString();
  }
}
