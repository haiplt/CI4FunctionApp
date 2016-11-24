// GenericWebhookCSharpSQLToAzureBlob

/*
{
  "CloudStorageAccountName": "azureblob4haiphan",
  "CloudStorageAccountKey": "2gfNwP0PZwBu6t9he8/UJA98bPgJFhz3yF3sy31PTnePNU3T4ADQyO8fCABjoIsqaHdLH7r1Y3uv2Tf64Xserg==",
  "CloudBlobContainerName": "blobcontainer01",

  "IntSqlUserID": "dataintegration",
  "IntSqlPassword": "Dkqhmh!As.Pxq?'H$'sx",
  "IntSqlDatabaseName": "dataintegration-clientname",
  "IntSqlAzureServer": "dev-data-integration.database.windows.net",

  "DataSqlUserID": "dataintegration",
  "DataSqlPassword": "Dkqhmh!As.Pxq?'H$'sx",
  "DataSqlDatabaseName": "test-contracts-hai",
  "DataSqlAzureServer": "dev-data-integration.database.windows.net",
  
  "DataSqlDateTimeOffset": "-10",
  "Separator": "|",
  "TextQualifier": "\"",
  "IncludeHeaders": "false",
  "EndOfLine": "",
  "FileName": "contracts-segments-2016-11-06T091924Z.csv",
  "LogAs": "contracts-segments",
  "DateFormat": "ddMMyyyy",
  "DecimalFormat": "0.00"
}

{
  "CloudStorageAccountName": "azureblob4haiphan",
  "CloudStorageAccountKey": "2gfNwP0PZwBu6t9he8/UJA98bPgJFhz3yF3sy31PTnePNU3T4ADQyO8fCABjoIsqaHdLH7r1Y3uv2Tf64Xserg==",
  "CloudBlobContainerName": "blobcontainer01",

  "IntSqlUserID": "dataintegration",
  "IntSqlPassword": "Dkqhmh!As.Pxq?'H$'sx",
  "IntSqlDatabaseName": "dataintegration-clientname",
  "IntSqlAzureServer": "dev-data-integration.database.windows.net",

  "DataSqlUserID": "dataintegration",
  "DataSqlPassword": "Dkqhmh!As.Pxq?'H$'sx",
  "DataSqlDatabaseName": "test-contracts-hai",
  "DataSqlAzureServer": "dev-data-integration.database.windows.net",

  "DataSqlExtractStoredProc": "[dbo].[spGetContractsByDate]",
  "DataSqlDateTimeOffset": -10,
  
  "Separator": "|",
  "TextQualifier": "\"",
  "IncludeHeaders": "false",
  "EndOfLine": "",
  "FileName": "contracts-segments-2016-11-06T091924Z.csv",
  "LogAs": "contracts-segments",
  "DateFormat": "ddMMyyyy",
  "DecimalFormat": "0.00"
 
}
*/

#r "Newtonsoft.Json"
#r "Microsoft.Azure.KeyVault.Core.dll"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.WindowsAzure.StorageClient.dll"
#r "System.Data.dll"

using System;
using System.Text;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Configuration;
using Newtonsoft.Json;
using Microsoft.Azure;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;
using System.Data;
using System.ComponentModel;


public static System.Data.DataTable ToDataTable<T>(this IEnumerable<T> data)
{
    PropertyDescriptorCollection properties =
        TypeDescriptor.GetProperties(typeof(T));
    System.Data.DataTable table = new DataTable();
    foreach (System.ComponentModel.PropertyDescriptor prop in properties)
        table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
    foreach (T item in data)
    {
        System.Data.DataRow row = table.NewRow();
        foreach (System.ComponentModel.PropertyDescriptor prop in properties)
            row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
        table.Rows.Add(row);
    }
    return table;
}


public static async Task<object> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info($"---------- Function started  ----------------------------------------------------");
    log.Info($"GenericWebhookCSharpSQLToAzureBlob was triggered!");



    string exportResult = string.Empty;
    string jsonContent = await req.Content.ReadAsStringAsync();
    dynamic data = JsonConvert.DeserializeObject(jsonContent);
    FileExporting objExport = new FileExporting();
    ExportReuslt objExportResult = new ExportReuslt();
    log.Info(jsonContent);
    log.Info($"Initiating variables");
    if (!objExport.InitVariable(data))
    {
        log.Info(objExport.ExceptionDetails);
        return req.CreateResponse(HttpStatusCode.BadRequest, new
        {
            error = objExport.ExceptionDetails
        });
    }
    else
    {
        Newtonsoft.Json.Linq.JObject jLogObject = (Newtonsoft.Json.Linq.JObject)Newtonsoft.Json.Linq.JToken.FromObject(objExport);
        log.Info("FileExporting after initial Variables");
        log.Info(jLogObject.ToString());
        //Full workflow for upload file : 
        log.Info($"1.Check Connecting for AzureSql, Azure Storage ");
        // Check connection to integration database 
        if (objExport.CheckAzureSqlConnectionOfData())
        {
            log.Info($" Check connection to Integration Database : Connected");

            if (objExport.CheckAzureSqlConnectionOfInt())
            {
                log.Info($"Check Connecting for AzureSqlConnection : Connected");
                if (objExport.CheckCloudStorageConnection())
                {
                    log.Info($" Check connection to Data Source Database : Connected");
                    //Exporting file to TempStorage  
                    //Get all contract
                    //Insert to Staging

                    if (objExport.TransformDataToStagingContractList())
                    {
                        log.Info($"     Done Transform Data from reporting DB To StagingContractList");
                        //Get update date --> csv
                        if (objExport.Exporting())
                        {
                            log.Info($"     Done Exporting filename  : " + objExport.FileName);

                            if (objExport.NumberOfRows > 0)
                            {
                                if (objExport.UploadToBlobStorage())
                                {
                                    log.Info($"         Uploaded filename: { objExport.FileName } to Blob Storage");
                                    objExport.ExtractStatus = "Completed";
                                }
                                else
                                {
                                    log.Info($"     Uploaded failure!");
                                    log.Info($"     ExceptionDetail : {objExport.ExceptionDetails}");
                                }

                            }
                            else
                            {
                                objExport.ExtractStatus = "Completed";
                                log.Info($"     No data updating");
                            }
                            //4.Write log info to SQL table.
                            if (objExport.WriteLog())
                                log.Info($"         Write log to database: Successful");
                            else
                                log.Info($"         Error when write log to database. Exception detail: {objExport.ExceptionDetails}");

                        }
                        else
                        {
                            log.Info($" Fails in Exporting filename : {objExport.FileName},");
                            log.Info($" ExceptionDetail : {objExport.ExceptionDetails}");
                        }

                        //Move staging to Temp
                        if (objExport.SaveContractListToTemp())
                        {
                            log.Info($" Log SaveContractListToTemp saving.");
                        }
                        else
                        {
                            log.Info($"     SaveContractListToTemp failure!");
                            log.Info($"     ExceptionDetail : {objExport.ExceptionDetails}");
                        }
                    }
                    else
                    {
                        log.Info($"     TransformDataToStagingContractList failure!");
                        log.Info($"     ExceptionDetail : {objExport.ExceptionDetails}");
                    }
                }
                else
                {
                    log.Info($"Check Connecting for Azure Storage : Can't Connecting to Azure Storage");
                    log.Info($"Exception detail: {objExport.ExceptionDetails}");
                }
            }
            else
                log.Info($"Check connection to Data Source Database : Can't Connect");
            log.Info(objExport.ExceptionDetails.ToString());
        }
        else
            log.Info($"Check connection to Integration Database : Can't Connect");

        exportResult = objExport.ExtractStatus;
        objExportResult.Filename = objExport.FileName;
        objExportResult.NumberOfRows = objExport.NumberOfRows;
        objExportResult.ExportStatus = objExport.ExtractStatus;
        objExportResult.ExceptionDetails = objExport.ExceptionDetails;

        Newtonsoft.Json.Linq.JObject jObject = (Newtonsoft.Json.Linq.JObject)Newtonsoft.Json.Linq.JToken.FromObject(objExportResult);
        exportResult = jObject.ToString();
        log.Info($"Export Result: {exportResult}");
        log.Info($"---------- Function completed ----------------------------------------------------");
        return req.CreateResponse(HttpStatusCode.OK, jObject);
    }
}

public class ExportReuslt
{
    public string Filename { get; set; }
    public int NumberOfRows { get; set; }
    public string ExportStatus { get; set; }
    public string ExceptionDetails { get; set; }
}

public class StagingContract
{
    public StagingContract() { }
    public string ContractNumberSegmentNumber { get; set; }
    public decimal SegmentValue { get; set; }
    public string ContractStatus { get; set; }
    public string ContractTitle { get; set; }
    public string SegmentTitle { get; set; }
    public DateTime SegmentEndDate { get; set; }
    public string ContractLegacyFMISNumber { get; set; }
    public bool Retired { get; set; }
    public bool Deleted { get; set; }

}

public class FileExporting
{
    #region Declare Vaiable & Property
    public DateTime ExtractDatetime { get; set; }
    public DateTime LoggingDateTime { get; set; }
    public int NumberOfRows { get; set; }
    public string ExtractStatus { get; set; }
    public string LogName { get; set; }

    // File setting
    public string CurrentDirectory { get; set; }
    public string ExceptionDetails { get; set; }

    public string RootDirectory = "D:\\home\\site\\wwwroot\\GenericWebhookCSharpSQLToAzureBlob\\"; //D:\home\site\wwwroot\<function_name>               
    public string FileName { get; set; }
    public string Separator { get; set; }
    public string TextQualifier { get; set; }
    public string DateFormat { get; set; }
    public string DecimalFormat { get; set; }
    public bool IncludeHeaders { get; set; }
    public string EndOfLine { get; set; }

    //Azure Sql Serve 
    public string IntSqlAzureServer { get; set; }
    public string IntSqlUserID { get; set; }
    public string IntSqlPassword { get; set; }
    public string IntSqlDatabaseName { get; set; }

    //PRG_Contracts Sql Serve 
    public string DataSqlAzureServer { get; set; }
    public string DataSqlUserID { get; set; }
    public string DataSqlPassword { get; set; }
    public string DataSqlDatabaseName { get; set; }
    public string DataSqlExtractStoredProc { get; set; }
    public int DataSqlDateTimeOffset { get; set; }

    private SqlConnection GetSqlConnectionOfIntSql
    {
        get
        {
            SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();

            //The name of the Azure SQL Database server
            //Example: myFirstDatabase.database.windows.net
            connectionBuilder["Server"] = IntSqlAzureServer;

            //User ID of the entity attempting to connect to the database
            connectionBuilder["User ID"] = IntSqlUserID;

            //The password associated the User ID
            connectionBuilder["Password"] = IntSqlPassword;

            //Name of the target database
            connectionBuilder["Database"] = IntSqlDatabaseName;

            //Denotes that the User ID and password are specified in the connection
            connectionBuilder["Integrated Security"] = false;

            //By default, Azure SQL Database uses SSL encryption for all data sent between
            //the client and the database
            connectionBuilder["Encrypt"] = true;
            SqlConnection connection = new SqlConnection(connectionBuilder.ToString());
            return connection;
        }
    }

    private SqlConnection GetSqlConnectionOfDataSql
    {
        get
        {
            SqlConnectionStringBuilder connectionBuilder = new SqlConnectionStringBuilder();

            //The name of the Azure SQL Database server
            //Example: myFirstDatabase.database.windows.net
            connectionBuilder["Server"] = DataSqlAzureServer;

            //User ID of the entity attempting to connect to the database
            connectionBuilder["User ID"] = DataSqlUserID;

            //The password associated the User ID
            connectionBuilder["Password"] = DataSqlPassword;

            //Name of the target database
            connectionBuilder["Database"] = DataSqlDatabaseName;

            //Denotes that the User ID and password are specified in the connection
            connectionBuilder["Integrated Security"] = false;

            //By default, Azure SQL Database uses SSL encryption for all data sent between
            //the client and the database
            connectionBuilder["Encrypt"] = true;
            SqlConnection connection = new SqlConnection(connectionBuilder.ToString());
            return connection;
        }
    }
    // for CloudStorage
    public string CloudStorageAccountName { get; set; }
    public string CloudStorageAccountKey { get; set; }
    public string UploadResult { get; set; }
    public string CloudStorageConnection { get; set; }
    public string CloudBlobContainerName { get; set; }
    public string TempCloudStorage { get; set; }
    public string CloudStorageConnectionString { get; set; }
    #endregion

    private Microsoft.WindowsAzure.Storage.Auth.StorageCredentials GetStorageCredentials
    {
        get
        {
            Microsoft.WindowsAzure.Storage.Auth.StorageCredentials credentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(CloudStorageAccountName, CloudStorageAccountKey);
            return credentials;
        }
    }

    public bool CheckAzureSqlConnectionOfData()
    {
        try
        {
            //Create a SqlConnection from the provided connection string
            using (SqlConnection connection = GetSqlConnectionOfDataSql)
            {
                //Open connection to database
                connection.Open();
                if (connection.State == System.Data.ConnectionState.Open)
                    connection.Close();
            }
            return true;
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
    }

    public bool CheckAzureSqlConnectionOfInt()
    {
        try
        {
            //Create a SqlConnection from the provided connection string
            using (SqlConnection connection = GetSqlConnectionOfIntSql)
            {
                //Open connection to database
                connection.Open();
                if (connection.State == System.Data.ConnectionState.Open)
                    connection.Close();
            }
            return true;
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
    }
    public bool CheckCloudStorageConnection()
    {
        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(GetStorageCredentials, true);    //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobContainer container = blobClient.GetContainerReference(CloudBlobContainerName);

        try
        {
            container.GetPermissions();
            return true;

        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
        }
        return false;
    }
    public bool UploadToBlobStorage()
    {
        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(CloudStorageAccountName, CloudStorageAccountKey), true);
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobContainer container = blobClient.GetContainerReference(CloudBlobContainerName);
        Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blockBlob = container.GetBlockBlobReference(FileName);

        try
        {
            using (var fileStream = System.IO.File.OpenRead(TempCloudStorage + "\\" + FileName))
            {
                blockBlob.UploadFromStream(fileStream);
            }
        }
        catch (Microsoft.WindowsAzure.StorageClient.StorageClientException exception)
        {
            ExceptionDetails = exception.Message.ToString();
            return false;
        }

        //Delete Blobs file after upload to Azure Storage success
        System.IO.File.Delete(TempCloudStorage + "\\" + FileName);

        return true;
    }

    public bool TransformDataToStagingContractList()
    {
        string cmdGetContract = "[dbo].[spGetAllContracts]";          
        List<StagingContract> listContract = new List<StagingContract>();
        StagingContract objContract;

        //Get all contract
        //Insert to Staging
        //Get update date --> csv
        //Move staging to Temp
        try
        {
            //Create a SqlConnection from the provided connection string
            using (SqlConnection connection = GetSqlConnectionOfDataSql)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.CommandText = cmdGetContract;
                connection.Open();

                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    objContract = new StagingContract();
                    //[ContractNumberSegmentNumber], [SegmentValue], [ContractStatus], [ContractTitle], [SegmentTitle], [SegmentEndDate], [ContractLegacyFMISNumber], [Retired], [Deleted]
                    if (!reader.IsDBNull(0))
                        objContract.ContractNumberSegmentNumber = reader.GetString(0);
                    if (!reader.IsDBNull(1))
                        objContract.SegmentValue = reader.GetDecimal(1);
                    if (!reader.IsDBNull(2))
                        objContract.ContractStatus = reader.GetString(2);
                    if (!reader.IsDBNull(3))
                        objContract.ContractTitle = reader.GetString(3);
                    if (!reader.IsDBNull(4))
                        objContract.SegmentTitle = reader.GetString(4); ;
                    if (!reader.IsDBNull(5))
                        objContract.SegmentEndDate = reader.GetDateTime(5);
                    if (!reader.IsDBNull(6))
                        objContract.ContractLegacyFMISNumber = reader.GetString(6);
                    objContract.Retired = reader.GetBoolean(7);
                    objContract.Deleted = reader.GetBoolean(8);

                    listContract.Add(objContract);
                }
                connection.Dispose();
                reader.Dispose();
            }
            //Insert to Staging
            var objBulk = new BulkUploadToSql<StagingContract>()
            {
                InternalStore = listContract,
                TableName = "StagingContractList",
                CommitBatchSize = 1000,
                ConnectionString = GetSqlConnectionOfIntSql.ConnectionString
            };
            objBulk.Commit();

            return true;
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
    }

    public bool SaveContractListToTemp()
    {
        string cmdSaveContractToTemp = "[dbo].[spUpdateContractList]";
        try
        {
            using (SqlConnection connection = GetSqlConnectionOfIntSql)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.CommandText = cmdSaveContractToTemp;

                connection.Open();
                int i = command.ExecuteNonQuery();
                return true;
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
    }

    public bool Exporting()
    {
        string cmdGetChangeList = "SELECT [ContractNumberSegmentNumber], [SegmentValue], [ContractStatus], [ContractTitle], [SegmentTitle], [SegmentEndDate], [ContractLegacyFMISNumber], CASE WHEN COALESCE([Retired],0) = 1 THEN 1 ELSE 0 END AS [Retired], CASE WHEN COALESCE([Deleted],0) = 1 THEN 1 ELSE 0 END AS [Deleted] FROM (";
        cmdGetChangeList += "SELECT [ContractNumberSegmentNumber], [SegmentValue], [ContractStatus], [ContractTitle], [SegmentTitle], [SegmentEndDate], [ContractLegacyFMISNumber], [Retired], [Deleted] FROM [dbo].StagingContractList ";
        cmdGetChangeList += "EXCEPT ";
        cmdGetChangeList += "SELECT [ContractNumberSegmentNumber], [SegmentValue], [ContractStatus], [ContractTitle], [SegmentTitle], [SegmentEndDate], [ContractLegacyFMISNumber], [Retired], [Deleted] FROM [dbo].TempContractList";
        cmdGetChangeList += ") A";

        try
        {
            //Create a SqlConnection from the provided connection string
            using (SqlConnection connection = GetSqlConnectionOfIntSql)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.Text;
                command.CommandText = cmdGetChangeList;
                connection.Open();

                string line = string.Empty;
                string ThisSeparator = string.Empty;
                string dataType = string.Empty;
                int ColumnNumber = 0;
                StringBuilder strline = new StringBuilder();
                SqlDataReader reader = command.ExecuteReader();

                //Check reader has data or empty
                if (reader.HasRows)
                {
                    using (System.IO.StreamWriter file = new System.IO.StreamWriter(TempCloudStorage + "\\" + FileName, false))
                    {
                        //Read data from the query

                        if (IncludeHeaders)
                        {
                            foreach (System.Data.DataRow r in reader.GetSchemaTable().Rows)
                            {
                                if (string.IsNullOrEmpty(strline.ToString()))
                                    strline.Append(TextQualifier + r[0].ToString() + TextQualifier);
                                else
                                    strline.Append(Separator + TextQualifier + r[0].ToString() + TextQualifier);
                            }
                            strline.Append(EndOfLine);
                            file.WriteLine(strline.ToString());
                        }

                        NumberOfRows = 0;
                        while (reader.Read())
                        {
                            strline.Clear();
                            ColumnNumber = 0;
                            foreach (System.Data.DataRow r in reader.GetSchemaTable().Rows)
                            {
                                // determine if first column and whether to include separator   
                                if (string.IsNullOrEmpty(strline.ToString()))
                                    ThisSeparator = "";
                                else
                                    ThisSeparator = Separator;

                                dataType = reader.GetDataTypeName(ColumnNumber);

                                if (dataType == "decimal")
                                    strline.Append(ThisSeparator + reader.GetDecimal(ColumnNumber).ToString(DecimalFormat));
                                else if (dataType == "bit")
                                    strline.Append(ThisSeparator + reader[r[0].ToString()].ToString());
                                else if (dataType == "int")
                                    strline.Append(ThisSeparator + reader[r[0].ToString()].ToString());
                                else if (dataType == "datetime")
                                    strline.Append(ThisSeparator + reader.GetDateTime(ColumnNumber).ToString(DateFormat));
                                else
                                    strline.Append(ThisSeparator + TextQualifier + reader[r[0].ToString()].ToString() + TextQualifier);

                                ColumnNumber += 1;
                            }
                            strline.Append(EndOfLine);
                            file.WriteLine(strline.ToString());
                            NumberOfRows += 1;
                        }
                    }
                }
                else
                {
                    NumberOfRows = 0;
                    FileName = string.Empty;
                }
                connection.Dispose();
                return true;
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
    }

    public bool WriteLog()
    {
        string queryString = string.Empty;
        LoggingDateTime = DateTime.Now;
        try
        {
            using (SqlConnection connection = GetSqlConnectionOfIntSql)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.CommandText = "[dbo].[uspExportLoggingCreate]";

                command.Parameters.Add("@LogName", System.Data.SqlDbType.NVarChar);
                command.Parameters["@LogName"].Value = LogName;

                command.Parameters.Add("@FileName", System.Data.SqlDbType.NVarChar);
                command.Parameters["@FileName"].Value = FileName;

                command.Parameters.Add("@NumberOfRows", System.Data.SqlDbType.Int);
                command.Parameters["@NumberOfRows"].Value = NumberOfRows;

                command.Parameters.Add("@LoggingDateTime", System.Data.SqlDbType.DateTime);
                command.Parameters["@LoggingDateTime"].Value = LoggingDateTime;

                command.Parameters.Add("@ExtractStatus", System.Data.SqlDbType.NVarChar);
                command.Parameters["@ExtractStatus"].Value = ExtractStatus;

                command.Parameters.Add("@ExceptionDetails", System.Data.SqlDbType.NVarChar);
                command.Parameters["@ExceptionDetails"].Value = ExceptionDetails;
                connection.Open();
                int i = command.ExecuteNonQuery();
                return true;
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
    }
    public string CleanFileName(string fileName)
    {
        return System.IO.Path.GetInvalidFileNameChars().Aggregate(fileName, (current, c) => current.Replace(c.ToString(), string.Empty));
    }

    public bool CheckPropertyExist(dynamic obj, string propertyName, System.Collections.ArrayList arrPropertyExist)
    {    
        try
        {
            var value = obj[propertyName].Value;
            return true;
        }
        catch 
        {
            arrPropertyExist.Add(propertyName);
            return false;
        }
    }

    public bool InitVariable(dynamic data)
    {
        System.Collections.ArrayList arrPropertyExist = new System.Collections.ArrayList();
        System.Collections.ArrayList arrMissing = new System.Collections.ArrayList();
        string MissingProperties = string.Empty;
        string NotExistProperties = string.Empty;
        string strReturn = string.Empty;
        RootDirectory = "C:\\home\\site\\wwwroot\\GenericWebhookCSharpSQLToAzureBlob\\";  //Update late

        //Inital 
        CurrentDirectory = RootDirectory;
        UploadResult = "UploadResult.JSON";
        ExceptionDetails = string.Empty;
        ExtractStatus = "Failed";
        TempCloudStorage = CurrentDirectory + "TempStorage";

        try
        {
            if (CheckPropertyExist(data, "LogAs", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.LogAs.ToString()))
                    arrMissing.Add("LogAs");
                else
                    LogName = data.LogAs.ToString();
            }

            if (CheckPropertyExist(data, "IntSqlUserID", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.IntSqlUserID.ToString()))
                    arrMissing.Add("IntSqlUserID");
                else
                    IntSqlUserID = data.IntSqlUserID.ToString();
            }

            if (CheckPropertyExist(data, "IntSqlPassword", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.IntSqlPassword.ToString()))
                    arrMissing.Add("IntSqlPassword");
                else
                    IntSqlPassword = data.IntSqlPassword.ToString();
            }

            if (CheckPropertyExist(data, "IntSqlDatabaseName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.IntSqlDatabaseName.ToString()))
                    arrMissing.Add("IntSqlDatabaseName");
                else
                    IntSqlDatabaseName = data.IntSqlDatabaseName.ToString();
            }

            if (CheckPropertyExist(data, "IntSqlAzureServer", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.IntSqlAzureServer.ToString()))
                    arrMissing.Add("IntSqlAzureServer");
                else
                    IntSqlAzureServer = data.IntSqlAzureServer.ToString();
            }

            if (CheckPropertyExist(data, "DataSqlUserID", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DataSqlUserID.ToString()))
                    arrMissing.Add("DataSqlUserID");
                else
                    DataSqlUserID = data.DataSqlUserID.ToString();
            }

            if (CheckPropertyExist(data, "DataSqlPassword", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DataSqlPassword.ToString()))
                    arrMissing.Add("DataSqlPassword");
                else
                    DataSqlPassword = data.DataSqlPassword.ToString();
            }

            if (CheckPropertyExist(data, "DataSqlDatabaseName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DataSqlDatabaseName.ToString()))
                    arrMissing.Add("DataSqlDatabaseName");
                else
                    DataSqlDatabaseName = data.DataSqlDatabaseName.ToString();
            }

            if (CheckPropertyExist(data, "DataSqlAzureServer", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DataSqlAzureServer.ToString()))
                    arrMissing.Add("DataSqlAzureServer");
                else
                    DataSqlAzureServer = data.DataSqlAzureServer.ToString();
            }

            if (CheckPropertyExist(data, "DataSqlDateTimeOffset", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DataSqlDateTimeOffset.ToString()))
                    arrMissing.Add("DataSqlDateTimeOffset");
                else
                    DataSqlDateTimeOffset = data.DataSqlDateTimeOffset;
            }

            if (CheckPropertyExist(data, "CloudStorageAccountName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.CloudStorageAccountName.ToString()))
                    arrMissing.Add("CloudStorageAccountName");
                else
                    CloudStorageAccountName = data.CloudStorageAccountName.ToString();
            }

            if (CheckPropertyExist(data, "CloudStorageAccountKey", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.CloudStorageAccountKey.ToString()))
                    arrMissing.Add("CloudStorageAccountKey");
                else
                    CloudStorageAccountKey = data.CloudStorageAccountKey.ToString();
            }

            if (CheckPropertyExist(data, "CloudBlobContainerName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.CloudBlobContainerName.ToString()))
                    arrMissing.Add("CloudBlobContainerName");
                else
                    CloudBlobContainerName = data.CloudBlobContainerName.ToString();
            }

            if (CheckPropertyExist(data, "Separator", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.Separator.ToString()))
                    arrMissing.Add("Separator");
                else
                    Separator = data.Separator.ToString();
            }

            if (CheckPropertyExist(data, "TextQualifier", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.TextQualifier.ToString()))
                    arrMissing.Add("TextQualifier");
                else
                    TextQualifier = data.TextQualifier.ToString();
            }

            if (CheckPropertyExist(data, "IncludeHeaders", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.IncludeHeaders.ToString()))
                    arrMissing.Add("IncludeHeaders");
                else
                    IncludeHeaders = Convert.ToBoolean(data.IncludeHeaders.ToString());
            }


            if (CheckPropertyExist(data, "DateFormat", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DateFormat.ToString()))
                    arrMissing.Add("DateFormat");
                else
                    DateFormat = data.DateFormat.ToString();
            }

            if (CheckPropertyExist(data, "DecimalFormat", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.DecimalFormat.ToString()))
                    arrMissing.Add("DecimalFormat");
                else
                    DecimalFormat = data.DecimalFormat.ToString();
            }

            if (CheckPropertyExist(data, "FileName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.FileName.ToString()))
                    arrMissing.Add("FileName");
                else
                    FileName = CleanFileName(data.FileName.ToString());
            }

            if (arrPropertyExist.Count > 0)
            {
                NotExistProperties = arrPropertyExist[0].ToString();
                for (int i = 1; i < arrPropertyExist.Count; i++)
                    NotExistProperties += ", " + arrPropertyExist[i].ToString();
                ExceptionDetails += string.Format("List of parameters not exist: {0} of the input object.", NotExistProperties);

                return false;
            }
            else if (arrMissing.Count > 0)
            {
                MissingProperties = arrMissing[0].ToString();
                for (int i = 1; i < arrMissing.Count; i++)
                    MissingProperties += ", " + arrMissing[i].ToString();
                ExceptionDetails = string.Format("Missing value in properties: {0} of the input object.", MissingProperties);
                return false;
            }
            else
                return true;
        }
        catch (Exception ex)
        {
            ExceptionDetails = string.Empty;           
            ExceptionDetails += " Message of Exception :" + ex.Message.ToString();
            return false;
        }
    }

}


public class BulkUploadToSql<T>
{
    public IList<T> InternalStore { get; set; }
    public string TableName { get; set; }
    public int CommitBatchSize { get; set; } = 1000;
    public string ConnectionString { get; set; }

    public void Commit()
    {
        if (InternalStore.Count > 0)
        {
            System.Data.DataTable dt;
            int numberOfPages = (InternalStore.Count / CommitBatchSize) + (InternalStore.Count % CommitBatchSize == 0 ? 0 : 1);
            for (int pageIndex = 0; pageIndex < numberOfPages; pageIndex++)
            {
                dt = InternalStore.Skip(pageIndex * CommitBatchSize).Take(CommitBatchSize).ToDataTable();
                BulkInsert(dt);
            }
        }
    }

    public void BulkInsert(DataTable dt)
    {
        using (SqlConnection connection = new SqlConnection(ConnectionString))
        {
            SqlBulkCopy bulkCopy =
                new SqlBulkCopy
                (
                    connection,
                    SqlBulkCopyOptions.TableLock |
                    SqlBulkCopyOptions.FireTriggers |
                    SqlBulkCopyOptions.UseInternalTransaction,
                    null
                );

            bulkCopy.DestinationTableName = TableName;
            connection.Open();

            bulkCopy.WriteToServer(dt);
            connection.Close();
        }
    }

}
