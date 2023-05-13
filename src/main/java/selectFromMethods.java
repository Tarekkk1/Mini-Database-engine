package main.java;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class selectFromMethods {
    public static boolean clumnIndex2(Vector<String> colNames, String tableName) {

        // check if the column is indexed or not
        return true;

    }

    public static Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException, ClassNotFoundException, IOException {
        Vector<Hashtable<String, Object>> iterator = new Vector<>();
        validateSelectFromTable(arrSQLTerms, strarrOperators);
        String tableName = arrSQLTerms[0].get_strTableName();
        Table table = extractTable("src/main/resources/data/" + tableName + ".ser");
        // her we will use the index if possible

        Vector<String> vec = new Vector<>();
        for (SQLTerm var : arrSQLTerms) {
            vec.add(var.get_strColumnName());

        }

        if (clumnIndex2(vec, tableName)) {
            // Impelement the index

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
