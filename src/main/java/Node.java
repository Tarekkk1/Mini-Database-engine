package main.java;

import java.util.Date;
import java.util.Vector;

public class Node {

    Vector<Node> children;
    Boundaries boundaries;
    Vector<RowReference> points;

    public Node(Boundaries b,int maxPoints){
        this.boundaries = b;
        this.children = new Vector<Node>(8);
        this.points = new Vector<RowReference>(maxPoints+1);
    }

    public boolean isLeaf(){
        return children.isEmpty();
    }

    public void octSplit(){
        //split points into 8 groups
        //create 8 children
        //assign boundaries to children
        //assign points to children
        //clear points

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
    

    
}
