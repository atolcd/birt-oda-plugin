/*
 *************************************************************************
 * Copyright (c) 2010 <<Your Company Name here>>
 *  
 *************************************************************************
 */

package org.eclipse.datatools.connectivity.oda.pentaho.impl;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;

/**
 * Implementation class of IResultSetMetaData for an ODA runtime driver.
 * <br>
 * For demo purpose, the auto-generated method stubs have
 * hard-coded implementation that returns a pre-defined set
 * of meta-data and query results.
 * A custom ODA driver is expected to implement own data source specific
 * behavior in its place. 
 */
public class ResultSetMetaData implements IResultSetMetaData
{
    
	private String[] columnNames = null;
	private String[] columnLabels = null;
	private String[] columnTypes = null;

	ResultSetMetaData(String[] colNames, String[] colLabels, String[] colTypes) throws OdaException {
		if (colNames == null)
			throw new OdaException("common_ARGUMENT_CANNOT_BE_NULL"); //$NON-NLS-1$

		this.columnNames = colNames;
		this.columnTypes = colTypes;
		this.columnLabels = colLabels;
	}
	
	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnCount()
	 */
	public int getColumnCount() throws OdaException
	{
		return getNbColumns();
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnName(int)
	 */
	public String getColumnName( int index ) throws OdaException
	{
		return columnNames[index - 1];
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnLabel(int)
	 */
	public String getColumnLabel( int index ) throws OdaException
	{
		return columnLabels[index - 1];
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnType(int)
	 */
	public int getColumnType( int index ) throws OdaException
	{
		if (columnTypes[index - 1].equals("Numeric"))
			return java.sql.Types.DOUBLE;
		
		if (columnTypes[index - 1].equals("Integer"))
			return java.sql.Types.INTEGER;

		if (columnTypes[index - 1].equals("Date"))
			return java.sql.Types.DATE;

		return java.sql.Types.CHAR;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnTypeName(int)
	 */
	public String getColumnTypeName( int index ) throws OdaException
	{
        int nativeTypeCode = getColumnType( index );
        return Driver.getNativeDataTypeName( nativeTypeCode );
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnDisplayLength(int)
	 */
	public int getColumnDisplayLength( int index ) throws OdaException
	{
        // TODO replace with data source specific implementation

        // hard-coded for demo purpose
		return 8;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getPrecision(int)
	 */
	public int getPrecision( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getScale(int)
	 */
	public int getScale( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return -1;
	}

	/*
	 * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#isNullable(int)
	 */
	public int isNullable( int index ) throws OdaException
	{
        // TODO Auto-generated method stub
		return IResultSetMetaData.columnNullableUnknown;
	}
	
	public int getNbColumns() {
		return columnNames.length;
	}
    
}
