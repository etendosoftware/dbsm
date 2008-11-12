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
import org.apache.ddlutils.model.Trigger;

/**
 * Represents the removal of a table from a model.
 * 
 * @version $Revision: $
 */
public class RemoveTriggerChange implements ModelChange{
    
    /** The trigger. */
    private Trigger _trigger;
    
    /**
     * Creates a remove change object.
     * 
     * @param trigger The trigger
     */
    public RemoveTriggerChange(Trigger trigger) {
        _trigger = trigger;
    }

    /**
     * Returns the trigger. 
     * 
     * @return The trigger
     */
    public Trigger getTrigger()
    {
        return _trigger;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        Trigger trigger = database.findTrigger(_trigger.getName(), caseSensitive);

        database.removeTrigger(trigger);
    }        
    
    @Override
	public String toString()
    {
    	return "RemoveTriggerChange. Name: "+_trigger.getName();
    }
}
