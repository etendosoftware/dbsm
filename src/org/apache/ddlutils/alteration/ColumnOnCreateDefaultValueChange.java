package org.apache.ddlutils.alteration;


import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

/**
 * Represents the change of the onCreateDefault value of a column.
 * 
 * @version $Revision: $
 */
public class ColumnOnCreateDefaultValueChange extends TableChangeImplBase implements ColumnChange
{
    /** The column. */
    private Column _column;
    /** The new onCreateDefault value. */
    private String _newonCreateDefaultValue;

    /**
     * Creates a new change object.
     * 
     * @param table           The table of the column
     * @param column          The column
     * @param newOnCreateDefaultValue The new onCreateDefault value
     */
    public ColumnOnCreateDefaultValueChange(Table table, Column column, String newonCreateDefaultValue)
    {
        super(table);
        _column          = column;
        _newonCreateDefaultValue = newonCreateDefaultValue;
    }

    /**
     * Returns the column.
     *
     * @return The column
     */
    public Column getChangedColumn()
    {
        return _column;
    }

    /**
     * Returns the new onCreateDefault value.
     *
     * @return The new onCreateDefault value
     */
    public String getNewonCreateDefaultValue()
    {
        return _newonCreateDefaultValue;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        Table  table  = database.findTable(getChangedTable().getName(), caseSensitive);
        Column column = table.findColumn(_column.getName(), caseSensitive);

        column.setOnCreateDefault(_newonCreateDefaultValue);
    }  
    
    @Override
	public String toString()
    {
    	return "ColumnOnCreateDefaultChange. Column: "+_column.getName();
    } 
}
