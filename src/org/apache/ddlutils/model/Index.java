package org.apache.ddlutils.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.Serializable;
import java.util.ArrayList;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Base class for indices.
 * 
 * @version $Revision: $
 */
public class Index implements ConstraintObject, Cloneable, Serializable
{
    /** The name of the index. */
    protected String    _name;
    /** Te index is unique */
    protected boolean _unique = false;
    /** The columns making up the index. */
    protected ArrayList _columns = new ArrayList();

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return _name;
    }

    /**
     * {@inheritDoc}
     */
    public void setName(String name)
    {
        _name = name;
    }

    /**
     * {@inheritDoc}
     */
    public int getColumnCount()
    {
        return _columns.size();
    }

    /**
     * {@inheritDoc}
     */
    public IndexColumn getColumn(int idx)
    {
        return (IndexColumn)_columns.get(idx);
    }

    /**
     * {@inheritDoc}
     */
    public IndexColumn[] getColumns()
    {
        return (IndexColumn[])_columns.toArray(new IndexColumn[_columns.size()]);
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasColumn(Column column)
    {
        for (int idx = 0; idx < _columns.size(); idx++)
        {
            IndexColumn curColumn = getColumn(idx);

            if (column.equals(curColumn.getColumn()))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void addColumn(IndexColumn column)
    {
        if (column != null)
        {
            for (int idx = 0; idx < _columns.size(); idx++)
            {
                IndexColumn curColumn = getColumn(idx);

                if (curColumn.getOrdinalPosition() > column.getOrdinalPosition())
                {
                    _columns.add(idx, column);
                    return;
                }
            }
            _columns.add(column);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeColumn(IndexColumn column)
    {
        _columns.remove(column);
    }

    /**
     * {@inheritDoc}
     */
    public void removeColumn(int idx)
    {
        _columns.remove(idx);
    }
    
    public boolean isUnique() {
        return _unique;
    }
    
    public void setUnique(boolean unique) {
        _unique = unique;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof Index)
        {
            Index other = (Index)obj;

            return new EqualsBuilder().append(_name,    other._name)
                                      .append(_unique,    other._unique)
                                      .append(_columns, other._columns)
                                      .isEquals();
        }
        else
        {
            return false;
        }
    }
    
    public boolean equalsIgnoreCase(Index other)
    {
        if (other instanceof Index)
        {
            Index otherIndex = (Index)other;

            boolean checkName = (_name != null) && (_name.length() > 0) &&
                                (otherIndex._name != null) && (otherIndex._name.length() > 0);

            if ((!checkName || _name.equalsIgnoreCase(otherIndex._name)) &&
                (getColumnCount() == otherIndex.getColumnCount()))
            {
                if (_unique != other._unique) {
                    return false;
                }
                
                for (int idx = 0; idx < getColumnCount(); idx++)
                {
                    if (!getColumn(idx).equalsIgnoreCase(otherIndex.getColumn(idx)))
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return new HashCodeBuilder(17, 37).append(_name)
                                          .append(_unique)
                                          .append(_columns)
                                          .toHashCode();
    }    
    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Index [name=");
        result.append(getName());
        result.append("; unique =");
        result.append(isUnique());
        result.append("; ");
        result.append(getColumnCount());
        result.append(" columns]");

        return result.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    public String toVerboseString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Index [name=");
        result.append(getName());
        result.append("; unique =");
        result.append(isUnique());
        result.append("] columns:");
        for (int idx = 0; idx < getColumnCount(); idx++)
        {
            result.append(" ");
            result.append(getColumn(idx).toString());
        }

        return result.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException
    {
        Index result = new Index();

        result._name    = _name;
        result._unique  = _unique;
        result._columns = (ArrayList)_columns.clone();

        return result;
    }
}
