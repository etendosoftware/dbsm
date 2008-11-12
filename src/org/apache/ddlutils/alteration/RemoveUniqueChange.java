package org.apache.ddlutils.alteration;

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

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.Unique;

/**
 * Represents the removal of an unique from a table.
 * 
 * @version $Revision: $
 */
public class RemoveUniqueChange extends TableChangeImplBase
{
    /** The unique to be removed. */
    private Unique _unique;

    /**
     * Creates a new change object.
     * 
     * @param table The table to remove the unique from
     * @param unique The unique
     */
    public RemoveUniqueChange(Table table, Unique unique)
    {
        super(table);
        _unique = unique;
    }

    /**
     * Returns the unique.
     *
     * @return The unique
     */
    public Unique getUnique()
    {
        return _unique;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        Table table = database.findTable(getChangedTable().getName(), caseSensitive);
        Unique unique = table.findUnique(_unique.getName(), caseSensitive);

        table.removeUnique(unique);
    }    
    
    @Override
	public String toString()
    {
    	return "RemoveUniqueChange. Name: "+_unique.getName();
    }
}
