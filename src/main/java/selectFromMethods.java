package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class selectFromMethods {
    public static boolean clumnIndex2(Vector<String> colNames, String tableName) throws ClassNotFoundException, IOException {

        // check if the column is indexed or not
       Table table = updateMethods.getTablefromCSV(tableName);
        for (String colName : colNames) {
            if (table.index1!=colName && table.index2==colName && table.index3==colName){
                return false;
            }
        }
        return true;

    }

    public static Vector<SQLTerm> getSqlTerm(String x, SQLTerm[] terms){
        Vector<SQLTerm> result= new Vector<>();
        for (int i=0;i<terms.length;i++) {
            SQLTerm var= terms[i];
            if(var.get_strColumnName().equals(x)){
                result.add(var);
            }
        }
        return result;
    }


    public static Vector<RowReference> searchIndex(SQLTerm[] terms, Table table) throws IOException, ParseException, DBAppException, ClassNotFoundException{

        Object[] tableInfo = updateMethods.getTableInfoMeta(table.getTableName());
        Hashtable<String, Object> columnMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> columnMax = (Hashtable<String, Object>) tableInfo[2];
        Vector<RowReference> result= new Vector<>();
        for(SQLTerm term: terms){

            String col= term._strColumnName;
            String operator= term._strOperator;
            Object value= term._objValue;

            Object xMin= columnMin.get(table.index1);
            Object xMax= columnMax.get(table.index1);
            Object yMin= columnMin.get(table.index2);
            Object yMax= columnMax.get(table.index2);
            Object zMin= columnMin.get(table.index3);
            Object zMax= columnMax.get(table.index3);


            if(operator.equals("=")){
                if(col.equals(table.index1)){
                    xMin= value;
                    xMax= value;

                }
                else if(col.equals(table.index2)){
                    yMin= value;
                    yMax= value;
                }
                else{
                    zMin= value;
                    zMax= value;
                }
            }
            else if(operator.equals(">")){
                if(col.equals(table.index1)){
                    xMin= value;
                    xMax= columnMax.get(table.index1);

                }
                else if(col.equals(table.index2)){
                    yMin= value;
                    yMax= columnMax.get(table.index2);
                }
                else{
                    zMin= value;
                    zMax= columnMax.get(table.index3);
                }
                
            }
            else if(operator.equals(">=")){
                if(col.equals(table.index1)){
                    xMin= value;
                    xMax= columnMax.get(table.index1);

                }
                else if(col.equals(table.index2)){
                    yMin= value;
                    yMax= columnMax.get(table.index2);
                }
                else{
                    zMin= value;
                    zMax= columnMax.get(table.index3);
                }
            }
            else if(operator.equals("<")){
                if(col.equals(table.index1)){
                    xMin= columnMin.get(table.index1);
                    xMax= value;

                }
                else if(col.equals(table.index2)){
                    yMin= columnMin.get(table.index2);
                    yMax= value;
                }
                else{
                    zMin= columnMin.get(table.index3);
                    zMax= value;
                }
            }
            else if(operator.equals("<=")){
                if(col.equals(table.index1)){
                    xMin= columnMin.get(table.index1);
                    xMax= value;

                }
                else if(col.equals(table.index2)){
                    yMin= columnMin.get(table.index2);
                    yMax= value;
                }
                else{
                    zMin= columnMin.get(table.index3);
                    zMax= value;
                }
            }

             // else if(operator.equals("!=")){
            //     if(col.equals(table.index1)){
            //         xMin= null;
            //         xMax= null;

            //     }
            //     else if(col.equals(table.index2)){
            //         yMin= null;
            //         yMax= null;
            //     }
            //     else{
            //         zMin= null;
            //         zMax= null;
            //     }
            // }

            String indexPath = "src/main/resources/data/" + table.getTableName() + "index.ser";
            Node root= updateMethods.getNodefromDisk(indexPath);
            result.addAll(root.find(xMin, xMax, yMin, yMax, zMin, zMax));





        }
        return result;
    }





    public static Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {
        Vector<Hashtable<String, Object>> iterator = new Vector<>();
        validateSelectFromTable(arrSQLTerms, strarrOperators);
        String tableName = arrSQLTerms[0].get_strTableName();
        Table table = extractTable("src/main/resources/data/" + tableName + ".ser");
        // her we will use the index if possible

        Vector<String> colNames = new Vector<>();
        for (SQLTerm var : arrSQLTerms) {
            colNames.add(var.get_strColumnName());
        }

        Vector<Object> values = new Vector<>();
        for (SQLTerm var : arrSQLTerms) {
            values.add(var.get_objValue());

        }



        if (clumnIndex2(colNames, tableName)) {
            //get the columns using the index
            //deserialize the index
            String indexPath = "src/main/resources/data/" + tableName + "index.ser";
            Node root= (Node) updateMethods.getNodefromDisk(indexPath);
            Vector<SQLTerm> x= getSqlTerm(table.index1, arrSQLTerms);
            Vector<SQLTerm> y= getSqlTerm(table.index2, arrSQLTerms);
            Vector<SQLTerm> z= getSqlTerm(table.index3, arrSQLTerms);
        
            Object xMin= null;
            Object xMax= null;
            Object yMin= null;
            Object yMax= null;
            Object zMin= null;
            Object zMax= null;
            Object[] tableInfo = updateMethods.getTableInfoMeta(tableName);
            Hashtable<String, Object> columnMin = (Hashtable<String, Object>) tableInfo[1];
            Hashtable<String, Object> columnMax = (Hashtable<String, Object>) tableInfo[2];
            for(int i=0;i<arrSQLTerms.length;i++){
                SQLTerm var= arrSQLTerms[i];
                String operator= var.get_strOperator();
                if(operator.equals("=")){
                    if(i==0){
                        xMin= x;
                        xMax= x;

                    }
                    else if(i==1){
                        yMin= y;
                        yMax= y;
                    }
                    else{
                        zMin= z;
                        zMax= z;
                    }
                }
                else if(operator.equals(">")){
                    if(i==0){
                        xMin= x;
                        xMax= columnMax.get(table.index1);

                    }
                    else if(i==1){
                        yMin= y;
                        yMax= columnMax.get(table.index2);
                    }
                    else{
                        zMin= z;
                        zMax= columnMax.get(table.index3);
                    }
                    
                }
                else if(operator.equals(">=")){
                    if(i==0){
                        xMin= x;
                        xMax= columnMax.get(table.index1);

                    }
                    else if(i==1){
                        yMin= y;
                        yMax= columnMax.get(table.index2);
                    }
                    else{
                        zMin= z;
                        zMax= columnMax.get(table.index3);
                    }
                }
                else if(operator.equals("<")){
                    if(i==0){
                        xMin= columnMin.get(table.index1);
                        xMax= x;

                    }
                    else if(i==1){
                        yMin= columnMin.get(table.index2);
                        yMax= y;
                    }
                    else{
                        zMin= columnMin.get(table.index3);
                        zMax= z;
                    }
                }
                else if(operator.equals("<=")){
                    if(i==0){
                        xMin= columnMin.get(table.index1);
                        xMax= x;

                    }
                    else if(i==1){
                        yMin= columnMin.get(table.index2);
                        yMax= y;
                    }
                    else{
                        zMin= columnMin.get(table.index3);
                        zMax= z;
                    }
                }
                else if(operator.equals("!=")){
                    if(i==0){
                        xMin= null;
                        xMax= null;

                    }
                    else if(i==1){
                        yMin= null;
                        yMax= null;
                    }
                    else{
                        zMin= null;
                        zMax= null;
                    }
                }

            }
        Vector<RowReference> result= root.find(xMin, xMax, yMin, yMax, zMin, zMax);
        for(RowReference row: result){
            for(PageAndRow record: row.pageAndRow){
                String path= "src/main/resources/data/" + tableName + record.page + ".ser";
                Object cluster= record.clustringvalue;
                Vector<Hashtable<String, Object>> page = extractPage(path);
                int rowNumber= updateMethods.getRowTarek(page, table.getClusteringKey(), cluster);
                Hashtable<String,Object> resultRow= page.get(rowNumber);
                iterator.add(resultRow);

               
        }
    }
        
        } else {
            for (int i = 0; i < table.getPages().size(); i++) {
                String path = table.getPages().get(i).getPath();
                Vector<Hashtable<String, Object>> page = extractPage(path);
                for (Hashtable<String, Object> row : page) {

                    Vector<Boolean> bool = selectHelper(arrSQLTerms, row);
                    Boolean b = bool.get(0);
                    for (int j = 1; j < bool.size(); j++) {
                        switch (strarrOperators[j - 1]) {
                            case "AND":
                                b = b & bool.get(j);
                                break;
                            case "OR":
                                b = b | bool.get(j);
                                break;
                            case "XOR":
                                b = b ^ bool.get(j);
                                break;
                            default:
                                throw new DBAppException("Wrong Operator!");
                        }
                    }
                    if (b == true) {
                        iterator.add(row);
                    }
                }
            }
        }
        return iterator.iterator();
    }

    public static void validateSelectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if (arrSQLTerms.length - 1 != strarrOperators.length)
            throw new DBAppException("Number of terms and operators does not match.");

        for (String operator : strarrOperators)
            if (operator != "AND" && operator != "OR" && operator != "XOR")
                throw new DBAppException("No Valid Operator.");

        String TableName = arrSQLTerms[0].get_strTableName();
        for (SQLTerm term : arrSQLTerms)
            if (!term.get_strTableName().equals(TableName))
                throw new DBAppException("The table name in all terms must be the same.");

    }

    private static Table extractTable(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object o = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return (Table) o;
    }

    private static Vector<Hashtable<String, Object>> extractPage(String path)
            throws IOException, ClassNotFoundException {
        try (FileInputStream fileIn = new FileInputStream(path)) {
            ObjectInputStream objectIn = new ObjectInputStream(fileIn);
            Object o = objectIn.readObject();
            objectIn.close();
            fileIn.close();
            return (Vector<Hashtable<String, Object>>) o;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static Vector<Boolean> selectHelper(SQLTerm[] arrSQLTerms, Hashtable<String, Object> row)
            throws DBAppException {

        Vector<Boolean> bool = new Vector<Boolean>();
        for (SQLTerm sqlTerm : arrSQLTerms) {
            switch (sqlTerm._strOperator) {
                case "=":
                    if (row.get(sqlTerm._strColumnName).equals(sqlTerm._objValue))
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case "!=":
                    if (!row.get(sqlTerm._strColumnName).equals(sqlTerm._objValue))
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case "<=":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) <= 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case "<":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) < 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case ">=":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) >= 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                case ">":
                    if (((Comparable) row.get(sqlTerm._strColumnName))
                            .compareTo((Comparable) sqlTerm._objValue) > 0)
                        bool.add(true);
                    else
                        bool.add(false);
                    break;
                default:
                    throw new DBAppException("Wrong Operator!");
            }
        }
        return bool;
    }
}
