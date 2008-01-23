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
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.View;

/**
 * Represents the removal of a table from a model.
 * 
 * @version $Revision: $
 */
public class RemoveViewChange implements ModelChange{
    
    /** The view. */
    private View _view;
    
    /**
     * Creates a remove change object.
     * 
     * @param view The view
     */
    public RemoveViewChange(View view) {
        _view = view;
    }

    /**
     * Returns the view. 
     * 
     * @return The view
     */
    public View getView()
    {
        return _view;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        View view = database.findView(_view.getName(), caseSensitive);

        database.removeView(view);
    }    
}
