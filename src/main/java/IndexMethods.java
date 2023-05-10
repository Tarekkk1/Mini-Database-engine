package main.java;

import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Vector;

public class IndexMethods {

    public static void createIndex(String strTableName, String[] ColName)
            throws DBAppException, ClassNotFoundException, IOException, ParseException {
        // check if this index is previously created
        // check colname.size==3
        // check if colname is in the table
        // create node(boundaries)
        // insert table in tree
        // serialize tree

        if (ColName.length != 3) {
            throw new DBAppException("Error in input");
        }

        String path;
        path = "src/main/resources/data/" + strTableName + ".ser";
        Table table = updateMethods.getTablefromCSV(path);
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
                root.insert(j, i, x, y, z);
            }
        }

        String indexPath = "src/main/resources/data/" + strTableName + "index.ser";
        deleteFromMethods.writeIntoDiskMostafa(root, indexPath);

    }

}
