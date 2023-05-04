package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class DBApp implements DBAppInterface {

    public DBApp() {
    }

    public void init() {
    }

    public void createTable(String tableName, String clusteringKey,
            Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        try {
            Table.exceptions(colNameType, colNameMin, colNameMax);

            File tableDirectory = new File("src/main/resources/data/");
            String[] pages = tableDirectory.list();
            boolean flag = false;
            if (pages.length == 0) {
                flag = false;
            } else {
                for (int i = 0; i < pages.length; i++) {
                    String pageName = pages[i];
                    if (pageName.equals(tableName + ".ser")) {
                        flag = true;
                        break;
                    }
                }
            }
            if (flag) {
                throw new DBAppException("The table already exists!");
            }

            // if (tableDirectory.exists())
            // throw new DBAppException("Already Exists");
            // else
            // tableDirectory.mkdir();

            Table tableInstance = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

            try {
                FileOutputStream tableFile = new FileOutputStream("src/main/resources/data/" + tableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(tableFile);
                out.writeObject(tableInstance);
                out.close();
                tableFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            wirteonMetaData(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
        } catch (Exception e) {
            throw new DBAppException("Error in creating table");
        }

    }

    private void wirteonMetaData(String tableName, String clusteringKey,
            Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        try {

            FileReader oldMetaDataFile = new FileReader("src/main/resources/metadata.csv");

            BufferedReader br = new BufferedReader(oldMetaDataFile);
            StringBuilder metaData = new StringBuilder();

            String curLine;
            while ((curLine = br.readLine()) != null)
                metaData.append(curLine).append('\n');
            FileWriter metaDataFile = new FileWriter("src/main/resources/metadata.csv");
            for (String colName : colNameType.keySet()) {
                metaData.append(tableName).append(",");
                metaData.append(colName).append(",");
                metaData.append(colNameType.get(colName)).append(",");
                metaData.append(colName.equals(clusteringKey) ? "True" : "False").append(",");
                // metaData.append("False,");
                // metaData.append("False,");
                metaData.append(colNameMin.get(colName)).append(",");
                metaData.append(colNameMax.get(colName));
                metaData.append("\n");
            }
            metaDataFile.write(metaData.toString());
            metaDataFile.close();
        } catch (Exception e) {
            throw new DBAppException("Error in writing on metadata");
        }
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue)
            throws DBAppException {
        try

        {
            insertMethods.insertIntoTable(tableName, colNameValue);
        } catch (

        Exception e) {
            throw new DBAppException("Error in insert");
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue,
            Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        try {
            updateMethods.updateTable(tableName, clusteringKeyValue, columnNameValue);
        } catch (Exception e) {
            throw new DBAppException("Error in update");
        }
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        try {
            deleteFromMethods.deleteFromTable(tableName, columnNameValue);

        } catch (Exception e) {

            throw new DBAppException("Error in delete");
        }
    }

    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        // TODO Auto-generated method stub
        try {
            return selectFromMethods.selectFromTable(sqlTerms, arrayOperators);
        } catch (Exception e) {
            throw new DBAppException("Error in select");
        }

    }

}