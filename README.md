

## Project Description
This project involves building a small database engine with support for Octrees Indices. The main functionalities are:

Create tables: Store tables as pages on disk.
Insert tuples: Add tuples to the tables.
Delete tuples: Remove tuples from the tables.
Search linearly: Perform linear search in tables.
Create Octrees: Generate Octrees on demand.
Use Octrees: Utilize Octrees for optimization.
Note that this is a simplified database engine without foreign keys or referential integrity constraints. Only implement mentioned functionalities and document assumptions in the code.

# Tables
- Store each table as separate pages on disk.
- Supported data types: Integer, String, Double, Date.
# Pages
- Each page represents a separate file.
- Use Java's binary object file (.class) for emulating pages.
- Each page is a serialized Vector (java.util.Vector).
- Each tuple is stored as a separate object in the binary file.
- Load pages only when tuples are needed.
- Delete pages with all rows deleted.
- Handle full page insertion by shifting rows or creating a new page.
# Meta-Data File
- Store meta-data in a text file.
- Format: TableName, ColumnName, ColumnType, ClusteringKey, IndexName, IndexType, min, max.
 # Indices
- Use Octree for creating indices.
- Implement Octree as a generic tree.
- Associate each value in the Octree with a reference to the table's page on disk.
- Use the range of each column to create divisions on the index scale.
- Update indices when inserting or deleting tuples.
- Scan the entire table to build indices after creation.
- Save and load indices from disk on application startup.
- Utilize indices for query execution when possible.




