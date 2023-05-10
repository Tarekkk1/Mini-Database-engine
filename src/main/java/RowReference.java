package main.java;

public class RowReference{
    int pageNumber;
    int indexInPage;
    Object x;
    Object y;
    Object z;
    
    public RowReference(int pageNumber, int indexInPage, Object x, Object y, Object z) {
        this.pageNumber = pageNumber;
        this.indexInPage = indexInPage;
        this.x = x;
        this.y = y;
        this.z = z;
    }

   
}
