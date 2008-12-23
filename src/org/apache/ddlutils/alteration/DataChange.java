package org.apache.ddlutils.alteration;

import org.apache.ddlutils.model.DatabaseData;

/**
 * Marker interface for changes to a database model element.
 * 
 * @version $Revision: $
 */
public interface DataChange extends Change {
    /**
     * Applies this change to the given database.
     * 
     * @param database
     *            The database
     * @param caseSensitive
     *            Whether the case of names matters
     */
    public void apply(DatabaseData databaseData, boolean caseSensitive);
}
