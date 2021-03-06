package edu.buffalo.cse562.Operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.Eval;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sf.jsqlparser.schema.Table;

public class SelectionOperator extends Eval implements Operator {

    /* WHERE */
    Operator opt = null;
    Expression condition = null;
    HashMap<String, Integer> inputSchema = null;
    HashMap<String, Integer> outputSchema = null;
    Expression[] tuple = null;
    String alias = null;

    public SelectionOperator(Operator opt, ArrayList<Object> condition) {
        this.opt = opt;
        this.inputSchema=opt.getOutputSchema();
        outputSchema = opt.getOutputSchema();
        this.condition = (Expression) condition.get(0);
        //System.out.print("WHERE");
        //System.out.println(condition);

        if (opt instanceof BigDataScanOperator) {
            this.alias = ((BigDataScanOperator) opt).getAlias();
        }
    }

    public HashMap<String, Integer> getOutputSchema() {
        return outputSchema;
    }

    public String getAlias() {
        return this.alias;
    }

    public Expression[] getTuple() {
        tuple = opt.getTuple();
        //System.out.println("Selection tuple"+tuple);
        if (tuple == null) {
            return null;
        }
        //System.out.println(tuple);
        try {
            //System.out.println(condition);
            //System.out.println(condition);
            LeafValue lf = this.eval(condition);
            if (lf == null) {
                return null;
            }

            while (lf instanceof BooleanValue && !((BooleanValue) lf).getValue()) {
                tuple = opt.getTuple();
                if (tuple == null) {
                    return null;
                }
                try {
                    
                    lf = this.eval(condition);
                    //System.out.println(tuple);

                } catch (SQLException ex) {
                    Logger.getLogger(SelectionOperator.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (lf == null) {
                    return null;
                }
            }
            
            return tuple;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void reset() {
        opt.reset();
    }

    
    public LeafValue eval1(Column c) throws SQLException {
        // TODO Auto-generated method stub
        //System.out.println("column :"+c);
        Table t = c.getTable();
        //System.out.println("Table wholename, name, alias :::" + t.getWholeTableName() + t.getName() + t.getAlias());
        String col = c.getColumnName();
        String col1 = c.getColumnName();
        //System.out.print("SELCTION :: val is null");
        //System.out.print("SELECTION : value of column:"+col);
        //System.out.println("SELECTION :: value of tuple"+tuple);
        Expression val = this.tuple[inputSchema.get(col)];
        if (val == null) {
            if (t.getName() != null) {
                col = t.getName() + "." + col;
            }
            val = this.tuple[inputSchema.get(col)];
            if (val == null) {
                if (t.getName() != null) {
                    col = t.getName().toUpperCase() + "." + col1;
                }
            }
            val = this.tuple[inputSchema.get(col)];
            if (val == null) {
                System.out.print("SELCTION :: val is null");
                System.out.print("SELECTION : value of column:" + col);
                System.out.println("SELECTION :: value of tuple" + tuple);
            }
        }

        return (LeafValue) val;
    }
    @Override
    public LeafValue eval(Column c) throws SQLException {
    
        Table t = c.getTable();
    
        String col = c.getColumnName().toUpperCase();
        //String col1 = c.getColumnName();
        StringBuilder colName = null;
         
        Expression val = null;
        if(inputSchema.get(col)!= null){
        	val = this.tuple[inputSchema.get(col)];
        	 return (LeafValue) val;
        }
        else{
            if (t.getName() != null) {
            	colName = new StringBuilder(t.getName());
            	colName.append(".");
            	colName.append(col);
            	
            	if(inputSchema.get(colName.toString())!=null){
            		val = this.tuple[inputSchema.get(colName.toString())];
            		colName = null;
                	return (LeafValue) val;
            	}
                else {
                		colName = null;
                    	colName = new StringBuilder(t.getName().toUpperCase());
                    	colName.append(".");
                    	colName.append(col);
                    	
                    	if(inputSchema.get(colName.toString())!=null){
                    		val = this.tuple[inputSchema.get(colName.toString())];
                    		colName = null;
                        	return (LeafValue) val;
                    	}
                    	else{
                    		System.out.print("SELCTION :: val is null");
                            System.out.print("SELECTION : value of column:" + col);
                            System.out.println("SELECTION :: value of tuple" + tuple);
                    		return null;
                    	}
                }
            }
        }

        return null;
    }
}
