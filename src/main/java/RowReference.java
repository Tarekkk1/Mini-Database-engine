package main.java;

import java.util.Vector;

class pageandrow {
    int page;
    int row;

}

public class RowReference {
    Vector<pageandrow> pageandrowlist;
    Object x;
    Object y;
    Object z;

    public RowReference(Object x, Object y, Object z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Object getX() {
        return x;
    }

    public Object getY() {
        return y;
    }

    public Object getZ() {
        return z;
    }

}
