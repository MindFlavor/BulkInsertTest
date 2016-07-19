ALTER DATABASE BulkMe SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
DROP DATABASE BulkMe;
GO

CREATE DATABASE BulkMe;
GO

ALTER DATABASE BulkMe SET RECOVERY SIMPLE --FULL;
GO
BACKUP DATABASE BulkMe TO DISK='nul';
GO


USE BulkMe;
GO 

CREATE PARTITION FUNCTION mySampleRange (int)  
    AS RANGE LEFT FOR VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31)
GO  
CREATE PARTITION SCHEME mySampeScheme  
    AS PARTITION mySampleRange  
    ALL TO ([PRIMARY]);
GO  

-- Heap table, no partitioning
CREATE TABLE tblHeap(
	ID INT,
	Testo NVARCHAR(255),
	Dt DATETIME
);
GO

-- Heap table, with partitioning
CREATE TABLE tblHeapPartition(
	ID INT,
	Testo NVARCHAR(255),
	Dt DATETIME,
	PartitionColumn AS ID % 32 PERSISTED
)
ON mySampeScheme (PartitionColumn) ;  
GO

CREATE PROCEDURE spspInsertPartition(
	@ID INT,
	@Testo NVARCHAR(255),
	@Dt DATETIME)
AS BEGIN
INSERT INTO tblHeapPartition(ID, Testo, Dt)
VALUES(@ID, @Testo, @Dt);
END;
GO

CREATE PROCEDURE spspInsert(
	@ID INT,
	@Testo NVARCHAR(255),
	@Dt DATETIME)
AS BEGIN
INSERT INTO tblHeap(ID, Testo, Dt)
VALUES(@ID, @Testo, @Dt);
END;
GO

USE [master];
GO