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

import org.apache.ddlutils.DdlUtilsException;
import org.apache.ddlutils.model.Check;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

public class AddCheckChange extends TableChangeImplBase {

    /** The new check. */
    private Check _newCheck;

    /**
     * Creates a new change object.
     * 
     * @param table
     *            The table to add the check to
     * @param newCheck
     *            The new check
     */
    public AddCheckChange(Table table, Check newCheck) {
        super(table);
        _newCheck = newCheck;
    }

    /**
     * Returns the new check.
     * 
     * @return The new check
     */
    public Check getNewCheck() {
        return _newCheck;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive) {
        Check newCheck = null;

        try {
            newCheck = (Check) _newCheck.clone();
        } catch (CloneNotSupportedException ex) {
            throw new DdlUtilsException(ex);
        }
        database.findTable(getChangedTable().getName()).addCheck(newCheck);
    }

    @Override
    public String toString() {
        return "AddCheckChange. Name: " + _newCheck.getName();
    }

}
