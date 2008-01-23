/*
 * RuleProcessor.java
 *
 * Created on 6 de noviembre de 2007, 15:48
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
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
    public RuleProcessor(String sql)  {
        
        String field = "\\s*((.+?)(\\s+?[Aa][Ss]\\s+?(.+?))??)\\s*?((,)|(\\s[Ff][Rr][Oo][Mm]\\s+))";
        Pattern p = Pattern.compile("^\\s*[Ss][Ee][Ll][Ee][Cc][Tt]\\s" + field); //  
        
        Pattern pField = Pattern.compile("\\w+(\\.\\w+)?");
        Pattern pFieldas = Pattern.compile("\\w+");
        Pattern pTable = Pattern.compile("\\w+(\\s+\\w+)?");
        
        Matcher m = p.matcher(sql);
        if (m.find()) {

            addField(m.group(2), m.group(4));
            String sseparator = m.group(5);
            int offset = m.end();
            
            p = Pattern.compile(field);
            m = p.matcher(sql);

            while (",".equals(sseparator)) {
                if (m.find(offset)) {
                    if (pField.matcher(m.group(2)).matches() && (m.group(4) == null || pFieldas.matcher(m.group(4)).matches())) {
                        addField(m.group(2), m.group(4));
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
        for(ViewField f : _viewfields) {
            result.append(f.getField());
            result.append(" AS ");
            result.append(f.getFieldas());
            result.append(",");
        }
        result.append(");]");
        
        return result.toString();
    }
}
