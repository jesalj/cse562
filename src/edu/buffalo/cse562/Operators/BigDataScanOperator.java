package edu.buffalo.cse562.Operators;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.EquivalentFilters.FilterCol;
import edu.buffalo.cse562.EquivalentFilters.NewFilterCol;
import edu.buffalo.cse562.RATree.*;

public class BigDataScanOperator implements Operator {

	/* FROM */

	private File f = null;
	private BufferedReader input = null;
	private String alias = null;
	HashMap<String, Integer> inputSchema = null;
	private HashMap<String, Integer> outputSchema = null;
	String[] colNameList = null;
	Expression[] tuple = null;
	ArrayList<String> tupleCache = new ArrayList<String>();
	SqlExpression sqlExpr = null;
	NewFilterCol colFilter = null;
	int[] colMap = null;
	int[] dataTypeList = null;
	int inputSchemaSize = 0;

	public BigDataScanOperator(File f, ArrayList<ColumnDefinition> schema,
			String alias) {
		this.f = f;

		this.alias = alias;

		this.tuple = new Expression[schema.size()];

		/*Get Array of column Names from list of columndefinition*/
		this.colNameList = this.getColumnNameArray(schema);

		/*Get map of column names for a table*/
		colFilter = new NewFilterCol();
		colMap = colFilter.getCompMap(alias);

		/*Load schema into SqlExpression*/
		NewSqlExpression.loadSqlExpression(schema);

		/*Retireve size of schema*/
		this.inputSchemaSize =schema.size();
		this.inputSchema = new HashMap<String, Integer>();

		/*Creating Map inputschema as mapping of column name
		 *  and corresponding index
		 *  */
		for (int i = 0; i < this.inputSchemaSize; i++) {
			this.inputSchema.put(schema.get(i).getColumnName().toUpperCase(), i);
		}

		/*Assign output schema*/
		this.outputSchema = this.inputSchema;

		/*Enumerate each Column Data type*/
		dataTypeList = this.getDataTypeList(schema);

                
		this.reset();
	}

	public String getAlias() {
		return alias;
	}

	public HashMap<String, Integer> getOutputSchema() {
		return this.outputSchema;
	}


	public Expression[] getTuple() {
		String line = null;
		Expression expr = null;
		//this.tuple = new HashMap<String, Expression>(20);
		this.tuple = new Expression[this.inputSchemaSize];

		int index = 0;
		int count = 1;
		int colItr = 0;
		int inputSchemaItr = 0;

		ColumnDefinition colDef = null;

		if (input == null)
			return null;

		try {
			line = input.readLine();
			if(line==null)
				return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String[] tmp = this.splitLine(line);

		colItr = 0;

		
		while (inputSchemaItr < this.inputSchemaSize) {
			//colDef = this.inputSchema.get(inputSchemaItr);

			if (colMap == null
					|| (colMap != null && colMap[inputSchemaItr] == 1)) {
                                        //System.out.println(inputSchemaItr);
				expr = NewSqlExpression.getExpression(
						this.dataTypeList[inputSchemaItr], tmp[inputSchemaItr]);
				
				
				if (expr == null)
					return null;

				//tuple.put(this.colNameList[colItr], expr);
				tuple[inputSchemaItr] = expr;
				colItr++;
			} else {
				//tuple.put(this.colNameList[colItr], expr);
				tuple[colItr] = expr;
				colItr++;
			}

			count++;
			inputSchemaItr++;
		}

		return tuple;
	}

	/*public ArrayList<String> getColumnNameList(
			ArrayList<ColumnDefinition> inputSchema) {
		Iterator<ColumnDefinition> itr = null;
		ArrayList<String> colNameList = null;
		if (inputSchema == null)
			return null;
		else
			itr = inputSchema.iterator();

		colNameList = new ArrayList<String>();

		while (itr.hasNext()) {
			colNameList.add(itr.next().getColumnName());
		}

		return colNameList;
	}*/

	public String[] getColumnNameArray(ArrayList<ColumnDefinition> inputSchema) {
		Iterator<ColumnDefinition> itr = null;
		String[] colNameList = null;
		int i = 0;

		if (inputSchema == null)
			return null;
		else
			itr = inputSchema.iterator();

		colNameList = new String[inputSchema.size()];

		while (itr.hasNext()) {
			colNameList[i] = itr.next().getColumnName();
			i++;
		}

		return colNameList;
	}

	public int[] getDataTypeList(ArrayList<ColumnDefinition> inputSchema) {
		Iterator<ColumnDefinition> itr = null;
		int[] dataTypeList = null;
		ColumnDefinition colDef = null;
		String colDataType = null;
		int indexItr = 0;

		if (inputSchema == null)
			return null;
		else
			itr = inputSchema.iterator();
                //System.out.println(inputSchema);
		dataTypeList = new int[inputSchema.size()];

		while (itr.hasNext()) {

			colDef = itr.next();
			colDataType = colDef.getColDataType().getDataType();

			if (colDataType.contains("(")) {
				int index = colDataType.indexOf("(");
				StringBuffer colType = new StringBuffer(colDataType);
				colDataType = colType.substring(0, index).toUpperCase();
				colType = null;
			} else
				colDataType = colDataType.toUpperCase();

			if (colDataType.equals("INT"))
				dataTypeList[indexItr] = 1;
			else if (colDataType.equals("VARCHAR"))
				dataTypeList[indexItr] = 2;
			else if (colDataType.equals("CHAR"))
				dataTypeList[indexItr] = 3;
			else if (colDataType.equals("STRING"))
				dataTypeList[indexItr] = 4;
			else if (colDataType.equals("DECIMAL"))
				dataTypeList[indexItr] = 5;
			else if (colDataType.equals("DATE"))
				dataTypeList[indexItr] = 6;

			indexItr++;
		}

		return dataTypeList;
	}

	public void reset() {
		try {
			input = new BufferedReader(new FileReader(f));
		} catch (IOException e) {
			e.printStackTrace();
			input = null;
		}
	}

	public String[] splitLine(String line){
		int i = 0;
		int j = 0;
		int count = 0;
		int index = 0;
		String substr = null;
		StringBuilder tmp1 = new StringBuilder(line);
		String[] tmp =new String[this.inputSchemaSize];
		int len = 0;

		while(count<this.inputSchemaSize && tmp1.indexOf("|", i)>=0){
			index=  tmp1.indexOf("|", i);
			j = index;

			if((colMap != null && colMap[count] == 1))
				tmp[count] =tmp1.substring(i, j);
			else
				tmp[count] = "A";

			i = j+1;
			count++;
		}
		len = line.length();

		if(count<this.inputSchemaSize && colMap != null && colMap[count] == 1)
			tmp[count] = tmp1.substring(i, len);
		else if(count<this.inputSchemaSize)
			tmp[count] = "A";

		return tmp;
	}

}
