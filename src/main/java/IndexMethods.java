package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Vector;

import javax.management.ObjectName;

public class IndexMethods {

    public static void updateMetadata(String[] columns, String tableName) throws IOException {
        String[] output = new String[6];
        // try {
        String file = "src/main/resources/metadata.csv";
        FileReader reader = new FileReader(file);
        BufferedReader buff = new BufferedReader(reader);
        StringBuilder s = new StringBuilder();
        String line = buff.readLine();
        String indexName = columns[0] + "" + columns[1] + "" + columns[2] + "Index";
        String indexType = "Octree";
        FileWriter finalOne = new FileWriter("src/main/resources/metadata.csv");

        do {
            boolean inserted = false;
            System.out.println(line);

            String arr[] = line.split(",");
            for (int i = 0; i < columns.length; i++) {

                if (columns[i].equals(arr[1]) && arr[0].equals(tableName)) {
                    s.append(arr[0]).append(",");
                    s.append(arr[1]).append(",");
                    s.append(arr[2]).append(",");
                    s.append(arr[3]).append(",");
                    s.append(indexName).append(",");
                    s.append(indexType).append(",");
                    s.append(arr[6]).append(",");
                    s.append(arr[7]).append('\n');
                    inserted = true;
                    break;

                }

            }

            if (!inserted) {
                s.append(line).append('\n');

            }

        } while ((line = buff.readLine()) != null);

        finalOne.write(s.toString());
        finalOne.close();

        // } catch (Exception e) {
        // System.out.println("Couldn't open csv file");

        // }
        return;

    }

    private static boolean contains(String[] a, String s) {
        for (String i : a) {
            if (i.equals(s)) {
                return true;
            }
        }
        return false;

    }

    public static void createIndex(String strTableName, String[] ColName)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {

        // to check if the input is correct

        if (ColName.length != 3) {
            throw new DBAppException("Error in input");
        }

        String path;
        path = "src/main/resources/data/" + strTableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
        table.index1 = ColName[0];
        table.index2 = ColName[1];
        table.index3 = ColName[2];
        deleteFromMethods.serialize(table, path);

        Object[] tableInfo = updateMethods.getTableInfoMeta(strTableName);
        Boundaries boundaries = new Boundaries();

        Hashtable<String, Object> columnMin = (Hashtable<String, Object>) tableInfo[1];
        Hashtable<String, Object> columnMax = (Hashtable<String, Object>) tableInfo[2];

        boundaries.minX = columnMin.get(ColName[0]);
        boundaries.maxX = columnMax.get(ColName[0]);
        boundaries.minY = columnMin.get(ColName[1]);
        boundaries.maxY = columnMax.get(ColName[1]);
        boundaries.minZ = columnMin.get(ColName[2]);
        boundaries.maxZ = columnMax.get(ColName[2]);

        Node root = new Node(boundaries, insertMethods.readConfig()[1]);

        for (int i = 0; i < table.getPages().size(); i++) {

            String pagePath = "src/main/resources/data/" + strTableName + i + ".ser";
            Vector<Hashtable<String, Object>> records = updateMethods.getPagesfromCSV(pagePath);
            for (int j = 0; j < records.size(); j++) {
                Object x = records.get(j).get(ColName[0]);
                Object y = records.get(j).get(ColName[1]);
                Object z = records.get(j).get(ColName[2]);
                root.insert(records.get(j), i, x, y, z);
            }
        }

        String indexPath = "src/main/resources/data/" + strTableName + "index.ser";
        deleteFromMethods.serialize(root, indexPath);

        updateMetadata(ColName, strTableName);
    }

    public static Vector<Object> columnIndexs(Hashtable<String, Object> colName, String path)
            throws ClassNotFoundException, IOException {

        Table table = updateMethods.getTablefromCSV(path);
        if (table.index1 != null) {
            if (colName.containsKey(table.index1) || colName.containsKey(table.index2)
                    || colName.containsKey(table.index3)) {

                Vector<Object> returned = new Vector<>();
                returned.add(colName.get(table.index1));
                returned.add(colName.get(table.index2));
                returned.add(colName.get(table.index3));

                return returned;

            }
        }

        return null;

    }

    public static void updateIndex(String strTableName)
            throws ClassNotFoundException, IOException, DBAppException, ParseException {
        String path = "src/main/resources/data/" + strTableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
        if (table.index1 != null) {
            File f = new File("src/main/resources/data/" + strTableName + "index.ser");
            f.delete();
            String[] ColName = new String[] { table.index1, table.index2, table.index3 };
            table.index1 = null;
            table.index2 = null;
            table.index3 = null;
            deleteFromMethods.serialize(table, path);

            createIndex(strTableName, ColName);

        }
        return;

    }

}
