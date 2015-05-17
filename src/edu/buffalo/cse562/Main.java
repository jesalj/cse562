package edu.buffalo.cse562;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.Statement;
import edu.buffalo.cse562.EquivalentFilters.BreakSelect;
import edu.buffalo.cse562.EquivalentFilters.EnhancedBreakSelect;
import edu.buffalo.cse562.EquivalentFilters.FilterCol;
import edu.buffalo.cse562.EquivalentFilters.NewFilterCol;
import edu.buffalo.cse562.EquivalentFilters.PushDownSelect;
import edu.buffalo.cse562.EquivalentFilters.TransformJoin;
import edu.buffalo.cse562.Operators.Operator;
import edu.buffalo.cse562.RATree.*;
import edu.buffalo.cse562.SqlParser.*;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.*;

public class Main {

    public static void main(String[] args) {

        ArrayList<File> sqlFiles = new ArrayList<File>();
        ArrayList<File> createFiles = new ArrayList<File>();
        HashMap<String, File> fileMap = new HashMap<String, File>();
        SelectBody sb = null;
        HashMap<String, HashMap<String, ArrayList<Object>>> selectMap = null;
        SchemaCalculator sCalc = new SchemaCalculator();

        // QueryPlan qPlan=null;
        BigDataQueryPlan bigQPlan = null;
        RATree raTree = null;
        ArrayList<RATree> treeList = null;
        ArrayList<Statement> stmtList = null;
        QueryParser qp = new QueryParser();
        QueryParserLoad qpLoad = new QueryParserLoad();
        long start = System.nanoTime();
        File dataDir = null;
        File dbDir = null;
        Operator o = null;
        int load = 0;
        Environment myDbEnvironment = null;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--data")) {
                dataDir = new File(args[i + 1]);
                i++;
            } else if (args[i].equals("--db")) {
                dbDir = new File(args[i + 1]);
                i++;
            } else if (args[i].equals("--load")) {
                i++;
                createFiles.add(new File(args[i]));
                load = 1;
            } else {
                if(load!=1){
                    createFiles.add(new File(args[i]));
                    i++;
                }
                sqlFiles.add(new File(args[i]));
                NewFilterCol.setQuery(args[i]);

            }
        }

        if (load == 1) {
            for (File sql : createFiles) {
                if (stmtList == null) {
                    stmtList = qpLoad.readQueries(sql);
                } else {
                    stmtList.addAll(qpLoad.readQueries(sql));
                }
            }

            try {
                // Open the environment. Create it if it does not already exist.
                EnvironmentConfig envConfig = new EnvironmentConfig();
                envConfig.setAllowCreate(true);
                myDbEnvironment = new Environment(dbDir, envConfig);
                
                
                //iterate over create queries and create databases for each
                for (Statement stmt : stmtList) {
                    ArrayList<CreateTable> ctList = new ArrayList<CreateTable>();
                    ctList.add((CreateTable) stmt);
                    qpLoad.parseCreate(ctList,myDbEnvironment,dataDir);
                }
                
                if (myDbEnvironment != null) {
                    myDbEnvironment.close();
                }
            } catch (DatabaseException dbe) {
                System.out.println("Error in database / environment" + dbe);
            }

        } else {
            // Contains map of tables and their data file
            for (File data : dataDir.listFiles()) {

                if (data.getName().endsWith(".tbl")
                        || data.getName().endsWith(".dat")) {
                    fileMap.put(
                            data.getName().substring(0,
                                    data.getName().lastIndexOf('.')), data);
                }
            }

            // Contains CREATE/SELECT, parsedtable/parsed select pair
            HashMap<String, HashMap<String, Object>> queryMap = new HashMap<String, HashMap<String, Object>>();

            // Contains Table name, schema(@Overridden to string) pair
            HashMap<String, TablesO> parsedTables = new HashMap<>();

            bigQPlan = new BigDataQueryPlan();

            for (File sql : sqlFiles) {
                if (stmtList == null) {
                    stmtList = qp.readQueries(sql);
                } else {
                    stmtList.addAll(qp.readQueries(sql));
                }
            }

            for (File sql : createFiles) {
                if (stmtList == null) {
                    stmtList = qp.readQueries(sql);
                } else {
                    stmtList.addAll(qp.readQueries(sql));
                }
            }

            Main obj1 = new Main();
            treeList = new ArrayList<RATree>();
            for (Statement stmt : stmtList) {
                if (stmt instanceof Select) {
                	//System.out.println("Query:"+stmt);
                    sb = ((Select) stmt).getSelectBody();
                    if (!(sb instanceof Union)) {
                        selectMap = qp.parseSelect("S0", sb);
                        obj1.printMap(selectMap);
                        LoadOperators obj = new LoadOperators();
                        raTree = obj.load(selectMap);
                        treeList.add(raTree);
                        // qPlan.trav(raTree.getRoot());
                    } else if (sb instanceof Union) {
                        ArrayList<PlainSelect> psList = (ArrayList<PlainSelect>) (((Union) sb)
                                .getPlainSelects());
                        if (psList.size() < 2) {
                            // System.out.println("Invalid PlainSelects found in Union: "
                            // + sb);
                        } else {
                            System.out.println("Union detected");
                            for (PlainSelect ps : psList) {
                                selectMap = qp.parseSelect("S0",
                                        (SelectBody) ps);
                                obj1.printMap(selectMap);
                                LoadOperators obj = new LoadOperators();
                                raTree = obj.load(selectMap);
                                treeList.add(raTree);
                            }
                        }
                    }
                } else if (stmt instanceof CreateTable) {
                    ArrayList<CreateTable> ctList = new ArrayList<CreateTable>();
                    ctList.add((CreateTable) stmt);
                    queryMap.putAll(qp.parseCreate(ctList));
                }
            }
            // System.out.println(queryMap);
            String key = null;
            HashMap<String, TablesO> newTable = new HashMap<>();
            for (Map.Entry pairs : queryMap.entrySet()) {
                // System.out.println("\n****************MAP START*****************************");
                // System.out.println(pairs.getKey() + " = " +
                // pairs.getValue());
                key = (String) pairs.getKey();
                // Separate parsed table objects from map
                if (key.matches("CREATE(.*)")) {
                    newTable = (HashMap<String, TablesO>) pairs.getValue();
                    parsedTables.putAll(newTable);
                }
                // System.out.println("\n****************MAP END*****************************");
            }

            // DEBUG print for parsed tables
            for (Map.Entry entry : parsedTables.entrySet()) {
                // System.out.println("Parsed Tables"+ ++count);
                // System.out.println(entry.getKey());
                TablesO table = (TablesO) entry.getValue();

                if (fileMap.get(table.getTableName().toLowerCase()) != null) {
                    table.setFilePath(fileMap.get(table.getTableName()
                            .toLowerCase()));
                } else {
                    table.setFilePath(fileMap.get(table.getTableName()
                            .toUpperCase()));
                }

                // System.out.println(table.getTableName());
            }

            for (RATree tree : treeList) {

                for (int i = 0; i < 6; i++) {
                    sCalc.trav(tree.getRoot(), parsedTables);
                    tree.trav(tree.getRoot(), new EnhancedBreakSelect());
                    sCalc.trav(tree.getRoot(), parsedTables);

                    // System.out.println("1---------------------------------");
                    tree.trav(tree.getRoot(), new PushDownSelect());
                    // System.out.println("2---------------------------------");
                    tree.trav(tree.getRoot(), new PushDownSelect());
                    // System.out.println("3---------------------------------");
                    tree.trav(tree.getRoot(), new PushDownSelect());
                    // System.out.println("4---------------------------------");
                    tree.trav(tree.getRoot(), new PushDownSelect());
                    // System.out.println("5---------------------------------");

                }
                sCalc.trav(tree.getRoot(), parsedTables);
                tree.trav(tree.getRoot(), new TransformJoin());
                sCalc.trav(tree.getRoot(), parsedTables);
                tree.trav(tree.getRoot(), new PushDownSelect());

                o = bigQPlan.trav(tree.getRoot(), parsedTables);

                Expression[] tuple = null;
                HashMap<String, Integer> outputSchema = new HashMap<String, Integer>();
                String flush = "";
                String out = "";
                Expression echeck = null;
                int flag = 0;
                String strEcheck = null;
                StringBuilder strBuffFlush = new StringBuilder();

                while ((tuple = o.getTuple()) != null) {
                    flag = 1;

                    outputSchema = o.getOutputSchema();

                    for (Expression ex : tuple) {

                        strEcheck = ex.toString();

                        if (strEcheck != null) {
                            ;
                            if (strEcheck.equals("0")
                                    || strEcheck.equals("0.0")
                                    || strEcheck.equals("")) {
                                flag = 1;
                            } else {
                                flag = 0;
                                break;
                            }
                        } else {
                            flag = 1;
                        }

                    }

                    if (flag == 1) {

                        continue;
                    }

                    int schemaIndex = 0;

                    int size = outputSchema.size();
                    for (int i = 0; i < size; i++) {

                        out = tuple[i].toString();

                        if (out.startsWith("'")) {
                            out = out.substring(1, out.lastIndexOf("'"));
                        }

                        strBuffFlush.append(out);
                        strBuffFlush.append("|");
                    }
                    if (tuple != null && flag == 0) {

                        System.out.println(strBuffFlush.substring(0,
                                strBuffFlush.lastIndexOf("|")));
                    }

                    strBuffFlush = null;
                    strBuffFlush = new StringBuilder();
                }
            }
            long end = System.nanoTime();
        }
    }

    public void printMap(HashMap<String, HashMap<String, ArrayList<Object>>> map) {
        HashMap<String, ArrayList<Object>> subMap = null;

        for (String stmt : map.keySet()) {
            subMap = map.get(stmt);
            for (String key : subMap.keySet()) {

            }
        }
    }

}
