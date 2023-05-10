package main.java;

import java.util.Date;
import java.util.Vector;

public class Node {

    Vector<Node> children;
    Boundaries boundaries;
    Vector<RowReference> points;
    int maxPoints;

    public Node(Boundaries b, int maxPoints) {
        this.boundaries = b;
        this.children = new Vector<Node>(8);
        this.points = new Vector<RowReference>(maxPoints + 1);
    }

    public boolean isLeaf() {
        return children.isEmpty();
    }

    public void octSplit() {

        for (int i = 0; i < 8; i++) {
            Boundaries b = new Boundaries();
            b.minX = (i % 2 == 0) ? boundaries.minX : (getMedian(b.minX, b.maxX));
            b.maxX = (i % 2 == 0) ? getMedian(b.minX, b.maxX) : boundaries.maxX;
            b.minY = (i % 4 < 2) ? boundaries.minY : getMedian(b.minY, b.maxY);
            b.maxY = (i % 4 < 2) ? getMedian(b.minY, b.maxY) : boundaries.maxY;
            b.minZ = (i < 4) ? boundaries.minZ : getMedian(b.minZ, b.maxZ);
            b.maxZ = (i < 4) ? getMedian(b.minZ, b.maxZ) : boundaries.maxZ;
            Node child = new Node(b, 2);// to be considered
            children.add(child);
        }

        for (int i = 0; i < points.size(); i++) {
            RowReference point = points.get(i);
            int index = getChildNumber(point);
            children.get(index).points.add(point);
        }

        points.clear();

    }

    private int getChildNumber(RowReference point) {
        if (updateMethods.check(point.z, this.getMedian(boundaries.minZ, boundaries.maxZ)) <= 0) {
            if (updateMethods.check(point.y, this.getMedian(boundaries.minY, boundaries.maxY)) <= 0) {
                if (updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX)) <= 0) {
                    return 0;
                } else {
                    return 1;
                }
            } else {
                if (updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX)) <= 0) {
                    return 2;
                } else {
                    return 3;
                }
            }
        } else {
            if (updateMethods.check(point.y, this.getMedian(boundaries.minY, boundaries.maxY)) <= 0) {
                if (updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX)) <= 0) {
                    return 4;
                } else {
                    return 5;
                }
            } else {
                if (updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX)) <= 0) {
                    return 6;
                } else {
                    return 7;
                }
            }
        }
    }

    public Object getMedian(Object min, Object max) {
        if (min instanceof Integer) {
            return (Integer) min + ((Integer) max - (Integer) min) / 2;
        } else if (min instanceof Double) {
            return (Double) min + ((Double) max - (Double) min) / 2;
        } else if (min instanceof String) {
            char c1 = ((String) min).charAt(0);
            char c2 = ((String) max).charAt(0);
            int ascii1 = (int) c1;
            int ascii2 = (int) c2;
            int middle = (ascii1 + ascii2) / 2;
            return (char) middle;
        } else if (min instanceof Date) {
            return new Date((((Date) min).getTime() + ((Date) max).getTime()) / 2);
        } else {
            return null;
        }
    }

    public void insert(Object x, Object y, Object z, pageandrow place) {
        if (isLeaf()) {
            RowReference point = new RowReference(x, y, z);
            point.pageandrowlist.add(place);
            points.add(point);
            if (points.size() > maxPoints) {
                octSplit();
            }
        } else {
            int index = getChildNumber(new RowReference(x, y, z));
            children.get(index).insert(x, y, z, place);
        }

    }

    // public void insert(RowReference point){
    //     if(isLeaf()){
    //         points.add(point);
    //         if(points.size()>maxPoints){
    //             octSplit();
    //         }
    //     }
    //     else{
    //         int index = getChildNumber(point);
    //         children.get(index).insert(point);
    //     }
    // }

    public void insert(int row,int page, Object x,Object y,Object z){
        if(isLeaf()){
            RowReference exist = find(x, y, z);
            if(exist==null){
                RowReference point = new RowReference(x,y,z);
                point.pageAndRow=new Vector<>();
                point.pageAndRow.add(new PageAndRow(page,row));
                points.add(point);
                if(points.size()>maxPoints){
                    octSplit();
                }
            }
            else{
                exist.pageAndRow.add(new PageAndRow(page, row));
            }
        }
        else{
            int index = getChildNumber(new RowReference(x, y, z));
            children.get(index).insert(row, page, x, y, z);
        }
    }

    public RowReference find(Object x,Object y,Object z){
        if(isLeaf()){
            for(int i=0;i<points.size();i++){
                RowReference point = points.get(i);
                if(updateMethods.check(point.x, x)==0 && updateMethods.check(point.y, y)==0 && updateMethods.check(point.z, z)==0){
                    return point;
                }
            }
            return null;
        }
        else{
            int index = getChildNumber(new RowReference(x,y,z));
            return children.get(index).find(x, y, z);
        }
    }
    // public void insert(RowReference point){
    // if(isLeaf()){
    // points.add(point);
    // if(points.size()>maxPoints){
    // octSplit();
    // }
    // }
    // else{
    // int index = getChildNumber(point);
    // children.get(index).insert(point);
    // }
    // }

    // public RowReference find(Object x,Object y,Object z){
    // if(isLeaf()){
    // for(int i=0;i<points.size();i++){
    // RowReference point = points.get(i);
    // if(updateMethods.check(point.x, x)==0 && updateMethods.check(point.y, y)==0
    // && updateMethods.check(point.z, z)==0){
    // return point;
    // }
    // }
    // return null;
    // }
    // else{
    // int index = getChildNumber(new RowReference(0,0,x,y,z));
    // return children.get(index).find(x, y, z);
    // }
    // }

    public Vector<RowReference> find(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
        Vector<RowReference> result = new Vector<RowReference>();
        if (isLeaf()) {
            for (int i = 0; i < points.size(); i++) {
                RowReference point = points.get(i);
                if (updateMethods.check(point.x, minX) >= 0 && updateMethods.check(point.x, maxX) <= 0
                        && updateMethods.check(point.y, minY) >= 0 && updateMethods.check(point.y, maxY) <= 0
                        && updateMethods.check(point.z, minZ) >= 0 && updateMethods.check(point.z, maxZ) <= 0) {
                    result.add(point);
                }
            }
            return result;
        } else {
            for (int i = 0; i < children.size(); i++) {
                Node child = children.get(i);
                if (updateMethods.check(child.boundaries.minX, minX) >= 0
                        && updateMethods.check(child.boundaries.maxX, maxX) <= 0
                        && updateMethods.check(child.boundaries.minY, minY) >= 0
                        && updateMethods.check(child.boundaries.maxY, maxY) <= 0
                        && updateMethods.check(child.boundaries.minZ, minZ) >= 0
                        && updateMethods.check(child.boundaries.maxZ, maxZ) <= 0) {
                    result.addAll(child.find(minX, maxX, minY, maxY, minZ, maxZ));
                }
            }
            return result;
        }
    }






    public RowReference delete(Object minX, Object maxX, Object minY, Object maxY, Object minZ, Object maxZ) {
        if (isLeaf()) {
            for (int i = 0; i < points.size(); i++) {
                RowReference point = points.get(i);
                if (updateMethods.check(point.x, minX) >= 0 && updateMethods.check(point.x, maxX) <= 0
                        && updateMethods.check(point.y, minY) >= 0 && updateMethods.check(point.y, maxY) <= 0
                        && updateMethods.check(point.z, minZ) >= 0 && updateMethods.check(point.z, maxZ) <= 0) {
                    points.remove(i);
                    return point;
                }
            }
            return null;
        } else {
            for (int i = 0; i < children.size(); i++) {
                Node child = children.get(i);
                if (updateMethods.check(child.boundaries.minX, minX) >= 0
                        && updateMethods.check(child.boundaries.maxX, maxX) <= 0
                        && updateMethods.check(child.boundaries.minY, minY) >= 0
                        && updateMethods.check(child.boundaries.maxY, maxY) <= 0
                        && updateMethods.check(child.boundaries.minZ, minZ) >= 0
                        && updateMethods.check(child.boundaries.maxZ, maxZ) <= 0) {
                    return child.delete(minX, maxX, minY, maxY, minZ, maxZ);
                }
            }
            return null;
        }
    }


















    

    public void print() {
        if (isLeaf()) {
            System.out.println("Leaf");
            for (int i = 0; i < points.size(); i++) {
                RowReference point = points.get(i);
                System.out.println(point.x + " " + point.y + " " + point.z);
            }
        } else {
            System.out.println("Node");
            for (int i = 0; i < children.size(); i++) {
                Node child = children.get(i);
                child.print();
            }
        }
    }

    public void printBoundaries() {
        System.out.println("minX: " + boundaries.minX + " maxX: " + boundaries.maxX + " minY: " + boundaries.minY
                + " maxY: " + boundaries.maxY + " minZ: " + boundaries.minZ + " maxZ: " + boundaries.maxZ);
    }

    public void printPoints() {
        for (int i = 0; i < points.size(); i++) {
            RowReference point = points.get(i);
            System.out.println(point.x + " " + point.y + " " + point.z);
        }
    }

    public void printChildren() {
        for (int i = 0; i < children.size(); i++) {
            Node child = children.get(i);
            child.print();
        }
    }

}
