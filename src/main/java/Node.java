package main.java;

import java.util.Date;
import java.util.Vector;

public class Node {

    Vector<Node> children;
    Boundaries boundaries;
    Vector<RowReference> points;
    int maxPoints;

    public Node(Boundaries b,int maxPoints){
        this.boundaries = b;
        this.children = new Vector<Node>(8);
        this.points = new Vector<RowReference>(maxPoints+1);
    }
    

    public boolean isLeaf(){
        return children.isEmpty();
    }

    public void octSplit(){

        for(int i=0;i<8;i++){
            Boundaries b = new Boundaries();
            b.minX = (i%2==0)?boundaries.minX:(getMedian(b.minX,b.maxX));
            b.maxX = (i%2==0)?getMedian(b.minX,b.maxX):boundaries.maxX;
            b.minY = (i%4<2)?boundaries.minY:getMedian(b.minY, b.maxY);
            b.maxY = (i%4<2)?getMedian(b.minY, b.maxY):boundaries.maxY;
            b.minZ = (i<4)?boundaries.minZ:getMedian(b.minZ, b.maxZ);
            b.maxZ = (i<4)?getMedian(b.minZ, b.maxZ):boundaries.maxZ;
            Node child = new Node(b,2);//to be considered
            children.add(child);
        }

        for(int i=0;i<points.size();i++){
            RowReference point = points.get(i);
            int index = getChildNumber(point);
            children.get(index).points.add(point);
        }

        points.clear();

    }


    private int getChildNumber(RowReference point) {
        if(updateMethods.check(point.z, this.getMedian(boundaries.minZ, boundaries.maxZ))<=0){
            if(updateMethods.check(point.y, this.getMedian(boundaries.minY, boundaries.maxY))<=0){
                if(updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX))<=0){
                    return 0;
                }
                else{
                    return 1;
                }
            }
            else{
                if(updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX))<=0){
                    return 2;
                }
                else{
                    return 3;
                }
            }
        }
        else{
            if(updateMethods.check(point.y, this.getMedian(boundaries.minY, boundaries.maxY))<=0){
                if(updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX))<=0){
                    return 4;
                }
                else{
                    return 5;
                }
            }
            else{
                if(updateMethods.check(point.x, this.getMedian(boundaries.minX, boundaries.maxX))<=0){
                    return 6;
                }
                else{
                    return 7;
                }
            }
        }
    }

    public Object getMedian(Object min,Object max){
        if(min instanceof Integer){
            return (Integer)min+((Integer)max-(Integer)min)/2;
        }
        else if(min instanceof Double){
            return (Double)min+((Double)max-(Double)min)/2;
        }
        else if(min instanceof String){
            char c1= ((String)min).charAt(0);
            char c2= ((String)max).charAt(0);
            int ascii1 = (int) c1;
            int ascii2 = (int) c2;
            int middle = (ascii1 + ascii2) / 2;
            return (char) middle;
        }
        else if(min instanceof Date){
            return new Date((((Date)min).getTime()+((Date)max).getTime())/2);
        }
        else{
            return null;
        }
    }

    public void insert(RowReference point){
        if(isLeaf()){
            points.add(point);
            if(points.size()>maxPoints){
                octSplit();
            }
        }
        else{
            int index = getChildNumber(point);
            children.get(index).insert(point);
        }
    }
    

    
}
