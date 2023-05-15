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
import java.util.ArrayList;
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
                metaData.append("null,");
                metaData.append("null,");
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
            throws ClassNotFoundException, DBAppException, IOException, ParseException, Exception {

        insertMethods.insertIntoTable(tableName, colNameValue);

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

    public static void createIndex(String strTableName, String[] ColName)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {
        IndexMethods.createIndex(strTableName, ColName);
    }

    public static void main(String[] args) throws Exception {
        DBApp dbApp = new DBApp();
        // Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        // htblColNameType.put("id", "java.lang.Integer");
        // htblColNameType.put("name", "java.lang.String");
        // // htblColNameType.put("date", "java.util.Date");
        // htblColNameType.put("gpa", "java.lang.Integer");

        // Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
        // Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
        // htblColNameMax.put("id", "1000");
        // htblColNameMax.put("name", "ZZZZZZZZZZ");
        // htblColNameMax.put("gpa", "4");
        // // htblColNameMax.put("date", "2021-01-01");
        // htblColNameMin.put("name", "A");
        // // htblColNameMin.put("date", "2020-01-01");
        // htblColNameMin.put("gpa", "0");
        // htblColNameMin.put("id", "0");
        // dbApp.createTable("Teacher", "id", htblColNameType, htblColNameMin,
        // htblColNameMax);
        /////////////////////////////

        
        // createIndex("Teacher", new String[] {"id" , "name" , "gpa"});

        // insertions
        // Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        // htblColNameValue.put("id", new Integer(2));
        // htblColNameValue.put("name", new String("T"));
        // htblColNameValue.put("gpa", new Integer(2));
        // dbApp.insertIntoTable("Teacher", htblColNameValue);

        // htblColNameValue.put("date", new
        // SimpleDateFormat("yyyy-MM-dd").parse("2020-05-01"));
        // dbApp.insertIntoTable("Teacher", htblColNameValue);

        // // <1,2,3> page 0
        // <4,5,6> page 1

        // FileInputStream fileIn = new
        // FileInputStream("src/main/resources/data/Teacheridnamegpa.ser");
        // ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        // Node v = (Node) objectIn.readObject();
        // objectIn.close();
        // fileIn.close();

        // System.out.println(v.points.size());

        // search

        // FileInputStream fileIn = new
        // FileInputStream("src/main/resources/data/Teacher.ser");
        // ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        // Table v = (Table) objectIn.readObject();
        // objectIn.close();
        // fileIn.close();
        // insertMethods.updateRange(v, "src/main/resources/data/Teacher0.ser");

        // System.out.println(v.getPages().get(0).getMaxClustering());
        // System.out.println(v.get(0).get("id"));

        // delete
        // Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        // htblColNameValue.put("id", new Integer(1));
        // // htblColNameValue.put("name", new String("T10"));
        // // htblColNameValue.put("gpa", new Double(0.8));
        // // htblColNameValue.put("date", new
        // // SimpleDateFormat("yyyy-MM-dd").parse("2020-05-01"));

        // dbApp.deleteFromTable("Teacher", htblColNameValue);

        // when a record is deleted, values shift upwards leaving empty spaces in the
        // page from the bottom
        // value is re inserted in the correct place

        // update
        // Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
        // htblColNameValue.put("name", new String("T10"));
        // // htblColNameValue.put("gpa", new Double(0.8));
        // htblColNameValue.put("date", new
        // SimpleDateFormat("yyyy-MM-dd").parse("2020-06-01"));

        // dbApp.updateTable("Teacher", "10", htblColNameValue);

        // // //select
        // FileInputStream fileIn = new
        // FileInputStream("src/main/resources/data/Teacher0.ser");
        // ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        // Vector<Hashtable<String, Object>> v = (Vector<Hashtable<String, Object>>)
        // objectIn.readObject();
        // objectIn.close();
        // fileIn.close();

        // System.out.println(v.get(1).get("id"));

    }

}