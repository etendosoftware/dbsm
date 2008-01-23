package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Table;

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

/**
 * Represents the removal of an index from a table.
 * 
 * @version $Revision: $
 */
public class RemoveCheckChange extends TableChangeImplBase {
    
    /** The check to be removed. */
    private Check _check;

    /**
     * Creates a new change object.
     * 
     * @param table The table to remove the check from
     * @param index The check
     */
    public RemoveCheckChange(Table table, Check check)
    {
        super(table);
        _check = check;
    }

    /**
     * Returns the check.
     *
     * @return The check
     */
    public Check getCheck()
    {
        return _check;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        Table table = database.findTable(getChangedTable().getName(), caseSensitive);
        Check check = table.findCheck(_check.getName(), caseSensitive);

        table.removeCheck(check);
    }  
}
