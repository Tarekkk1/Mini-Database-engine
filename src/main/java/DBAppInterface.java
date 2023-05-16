package main.java;

import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;

public interface DBAppInterface {

        void init();

        void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
                        Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax)
                        throws DBAppException;

        void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
                        throws DBAppException, ClassNotFoundException, IOException, ParseException, Exception;

        void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
                        throws DBAppException, ClassNotFoundException, IOException, ParseException;

        void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue)
                        throws DBAppException, Exception;

        // Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws
        // DBAppException;

}
