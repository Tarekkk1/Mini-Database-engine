package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.management.ObjectName;
import javax.print.DocFlavor.STRING;

public class selectFromMethods {
    public static int clumnIndex2(Vector<String> colNames, String tableName)
            throws ClassNotFoundException, IOException {

        // check if the column is indexed or not
        Table table = updateMethods.getTablefromCSV(tableName);
        for (String colName : colNames) {
            for (int i = 0; i < table.indexs.size(); i++) {
                Index index = table.indexs.get(i);
                if (colName.contains(index.index1) && colName.contains(index.index2)
                        && colName.contains(index.index3)) {
                    return i;
                }
            }

        }
        return -1;

    }

    public static Vector<SQLTerm> getSqlTerm(String x, SQLTerm[] terms) {
        Vector<SQLTerm> result = new Vector<>();
        for (int i = 0; i < terms.length; i++) {
            SQLTerm var = terms[i];
            if (var.get_strColumnName().equals(x)) {
                result.add(var);
            }
        }
        return result;
    }

    public static Vector<RowReference> searchIndex(Vector<SQLTerm> terms, Table table, int number)
            throws IOException, ParseException, DBAppException, ClassNotFoundException {

        Object[] tableInfo = updateMethods.getTableInfoMeta(table.getTableName());
        Hashtable<String, Object> columnMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> columnMax = (Hashtable<String, Object>) tableInfo[2];

        Index index = table.indexs.get(number);
        Vector<RowReference> result = new Vector<>();
        for (SQLTerm term : terms) {

            String col = term._strColumnName;
            String operator = term._strOperator;
            Object value = term._objValue;

            Object xMin = columnMin.get(index.index1);
            Object xMax = columnMax.get(index.index1);
            Object yMin = columnMin.get(index.index2);
            Object yMax = columnMax.get(index.index2);
            Object zMin = columnMin.get(index.index3);
            Object zMax = columnMax.get(index.index3);

            if (operator.equals("=")) {
                if (col.equals(index.index1)) {
                    xMin = value;
                    xMax = value;

                } else if (col.equals(index.index2)) {
                    yMin = value;
                    yMax = value;
                } else {
                    zMin = value;
                    zMax = value;
                }
            } else if (operator.equals(">")) {
                if (col.equals(index.index1)) {
                    xMin = value;
                    xMax = columnMax.get(index.index1);

                } else if (col.equals(index.index2)) {
                    yMin = value;
                    yMax = columnMax.get(index.index2);
                } else {
                    zMin = value;
                    zMax = columnMax.get(index.index3);
                }

            } else if (operator.equals(">=")) {
                if (col.equals(index.index1)) {
                    xMin = value;
                    xMax = columnMax.get(index.index1);

                } else if (col.equals(index.index2)) {
                    yMin = value;
                    yMax = columnMax.get(index.index2);
                } else {
                    zMin = value;
                    zMax = columnMax.get(index.index3);
                }
            } else if (operator.equals("<")) {
                if (col.equals(index.index1)) {
                    xMin = columnMin.get(index.index1);
                    xMax = value;

                } else if (col.equals(index.index2)) {
                    yMin = columnMin.get(index.index2);
                    yMax = value;
                } else {
                    zMin = columnMin.get(index.index3);
                    zMax = value;
                }
            } else if (operator.equals("<=")) {
                if (col.equals(index.index1)) {
                    xMin = columnMin.get(index.index1);
                    xMax = value;

                } else if (col.equals(index.index2)) {
                    yMin = columnMin.get(index.index2);
                    yMax = value;
                } else {
                    zMin = columnMin.get(index.index3);
                    zMax = value;
                }
            }

            String indexPath = "src/main/resources/data/" + table.getTableName() + "index.ser";
            Node root = updateMethods.getNodefromDisk(indexPath);
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

        // solving the errors
        int number = clumnIndex2(colNames, tableName);

        if (number != -1) {
            Index index = table.indexs.get(number);
            String indexPath = index.path;
            Node root = (Node) updateMethods.getNodefromDisk(indexPath);

            Vector<SQLTerm> x = getSqlTerm(index.index1, arrSQLTerms);
            Vector<SQLTerm> y = getSqlTerm(index.index2, arrSQLTerms);
            Vector<SQLTerm> z = getSqlTerm(index.index3, arrSQLTerms);

            x.addAll(x.size() + 1, y);
            x.addAll(x.size() + 1, z);
            Vector<RowReference> result = searchIndex(x, table, number);
            Vector<Hashtable<String, Object>> result2 = new Vector<>();
            result2 = getRowsFromRefarance(result, table);
            for (Hashtable<String, Object> row : result2) {
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

    private static Vector<Hashtable<String, Object>> getRowsFromRefarance(Vector<RowReference> result, Table table)
            throws ClassNotFoundException, IOException {

        Vector<Hashtable<String, Object>> rows = new Vector<Hashtable<String, Object>>();
        for (RowReference rowReference : result) {

            for (PageAndRow pageAndRow : rowReference.pageAndRow) {
                String path = "src/main/resources/data/" + table.getName() + pageAndRow.page + ".ser";
                Vector<Hashtable<String, Object>> page = extractPage(path);
                int rowNumber = updateMethods.getRowTarek(page, table.getClusteringKey(),
                        pageAndRow.clustringvalue);
                Hashtable<String, Object> resultRow = page.get(rowNumber);
                rows.add(resultRow);
            }
        }
        return rows;

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
