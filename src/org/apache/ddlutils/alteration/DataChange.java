package org.apache.ddlutils.alteration;



import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.beanutils.DynaBean;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.DatabaseData;

/**
 * Marker interface for changes to a database model element.
 * 
 * @version $Revision: $
 */
public interface DataChange
{
    /**
     * Applies this change to the given database.
     * 
     * @param database      The database
     * @param caseSensitive Whether the case of names matters
     */
    public void apply(DatabaseData databaseData, boolean caseSensitive);
}
