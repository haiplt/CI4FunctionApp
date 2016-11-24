//GenericWebhookCSharpLoadCsvToSqlAzure

/* 
{    
  "CloudBlobContainerName": "blobcontainer01",
  "CloudStorageAccountName": "blobstorage4haiphan",
  "CloudStorageAccountKey": "oR8Kg8tQdFivtDJ9ZcJx61sp05hyfZVUxa2yH9Jv6SlHyGFPc7DTXf7xWYEH8GNruUCyLcXa1yN020ZAQOkoog==",

  "IntSqlUserID": "dataintegration",
  "IntSqlPassword": "Dkqhmh!As.Pxq?'H$'sx",
  "IntSqlDatabaseName": "dataintegration-clientname",
  "IntSqlAzureServer": "dev-data-integration.database.windows.net",

  "DataSqlUserID": "dataintegration",
  "DataSqlPassword": "Dkqhmh!As.Pxq?'H$'sx",
  "DataSqlDatabaseName": "test-contracts-hai",
  "DataSqlAzureServer": "dev-data-integration.database.windows.net",

  "BlobFileName": "financial-transactions-summary-by-segment-2016-11-11T102823Z.csv",
  "Separator": "|",
  "TextQualifier": "\"",
  "IncludeHeaders": "true",
  "MessageEmail": "haiplt@live.com",
  "ReceiverEmail": "Hai@singlecell.com.au; haiplt@live.com; haiplt@gmail.com"
}
*/

#r "CsvHelper.dll"
#r "Microsoft.Azure.KeyVault.Core.dll"
#r "Microsoft.CSharp.dll"
#r "Microsoft.Data.OData.dll"
#r "Microsoft.Data.Services.Client.dll"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.WindowsAzure.StorageClient.dll"
#r "Newtonsoft.Json.dll"
#r "System.Data.DataSetExtensions.dll"
#r "System.Data.dll"
#r "System.Net.Http.dll"
#r "System.Xml.Linq.dll"
#r "System.Core.dll"
#r "SendGrid.dll"


using System;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Configuration;
using Newtonsoft.Json;
using Microsoft.Azure;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections;
using SendGrid;
using SendGrid.Helpers.Mail;
using System.Threading.Tasks;

public static DataTable ToDataTable<T>(this IEnumerable<T> data)
{
    PropertyDescriptorCollection properties =
        TypeDescriptor.GetProperties(typeof(T));
    System.Data.DataTable table = new DataTable();
    foreach (PropertyDescriptor prop in properties)
        table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
    foreach (T item in data)
    {
        System.Data.DataRow row = table.NewRow();
        foreach (PropertyDescriptor prop in properties)
            row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
        table.Rows.Add(row);
    }
    return table;
}
public static async Task<object> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info($"---------- Function started  ----------------------------------------------------");
    log.Info($"GenericWebhookCSharpLoadCsvToSqlAzure was triggered!");
    ResponseReuslt objResult = new ResponseReuslt();
    BlobHelper objIntegrate = new BlobHelper();
    string jsonContent = await req.Content.ReadAsStringAsync();
    dynamic data = JsonConvert.DeserializeObject(jsonContent);
    string strResponseResult = string.Empty;
    //Validation that parameters exist
    if (!objIntegrate.InitVariable(data))
    {
        log.Info(objIntegrate.ExceptionDetails);
        return req.CreateResponse(HttpStatusCode.BadRequest, new
        {
            error = objIntegrate.ExceptionDetails
        });
    }
    else
    {
        string strSerializeObject = JsonConvert.SerializeObject(objIntegrate);
        log.Info("BlobHelper after initial Variables");
        log.Info(strSerializeObject);
        //Full workflow for upload file : 
        log.Info($"1. Check Connecting for AzureSql, Azure Storage");
        if (objIntegrate.CheckAzureSqlConnection())
        {
            log.Info($"Check Connecting for AzureSqlConnection : Connected");
            if (objIntegrate.CheckCloudStorageConnection())
            {
                log.Info($" Check Connecting for Azure Storage : Connected");
                log.Info($"2. Download file to TempStorage");
                objIntegrate.TruncateStagging();
                if (objIntegrate.GetFileFromCloudStorage(objIntegrate.BlobFileName))
                {
                    objIntegrate.GetFullPRG_ContractsAndSegmentNumber();
                    objIntegrate.IntegrateSatus = "Completed";
                    //Validating data
                    log.Info($"3. Loading data to SqlAzure");  
                    //Invalid data --> Generate log file and Email to Receiver
                    if (objIntegrate.LoadingData(objIntegrate.BlobFileName))
                    {
                        log.Info($"Done Loading filename : {objIntegrate.BlobFileName}");
                    }
                    else
                    {
                        objIntegrate.IntegrateSatus = "False";
                        log.Info($"Loading false filename : {objIntegrate.BlobFileName}");
                        log.Info($"Error when loading data : {objIntegrate.ExceptionDetails}");
                    }

                }
                else
                    log.Info($"Error when download file to TempStorage : {objIntegrate.ExceptionDetails}");
                //Delete Blobs file after upload to Azure Storage success
                System.IO.File.Delete(objIntegrate.CurrentFileNameFullPath);
            }
            else
            {
                log.Info($"Check Connecting for Azure Storage : Can't Connecting to Azure Storage");
                log.Info($"Exception detail: {objIntegrate.ExceptionDetails}");
            }
        }
        else
        {
            log.Info($"Check Connecting for SFTP : Can't Connecting to SFTP");
        }
        log.Info($"Integrate Satus : " + objIntegrate.IntegrateSatus);
        objIntegrate.CompletedDatetime = DateTime.Now;

        log.Info($"4.Write log info");
        //Update log detail if had exception happen.
        if (objIntegrate.IntegrateSatus == "False")
            objIntegrate.UpdateIntegrateDataLogging();

        objResult.FunctionName = objIntegrate.LogName;
        objResult.ExceptionDetails = objIntegrate.ExceptionDetails;
        objResult.CompletedDateTime = objIntegrate.CompletedDatetime;
        objResult.NumberOfRowsInFile = objIntegrate.NumberOfRowsInFile;
        objResult.NumberOfValidationErrors = objIntegrate.DetailsValidation.Count;
        objResult.SummaryOfValidationErrors = objIntegrate.InvalidSummary;
        objResult.FunctionStatus = objIntegrate.IntegrateSatus;
        Newtonsoft.Json.Linq.JObject jObject = (Newtonsoft.Json.Linq.JObject)Newtonsoft.Json.Linq.JToken.FromObject(objResult);
        strResponseResult = jObject.ToString(Newtonsoft.Json.Formatting.None);

        log.Info(strResponseResult);
        log.Info($"---------- Function completed ----------------------------------------------------");
        return req.CreateResponse(HttpStatusCode.OK, jObject);
    }
}

// Class to contain list of blob files info
public class ResponseReuslt
{
    public ResponseReuslt()
    {
        FunctionStatus = "False";
        NumberOfRowsInFile = 0;
        NumberOfValidationErrors = 0;
        SummaryOfValidationErrors = string.Empty;
        ExceptionDetails = string.Empty;
    }
    public string FunctionName { get; set; }
    //Completed/False
    public string FunctionStatus { get; set; }
    public DateTime CompletedDateTime { get; set; }
    public int NumberOfRowsInFile { get; set; }
    public int NumberOfValidationErrors { get; set; }
    public string SummaryOfValidationErrors { get; set; }
    public string ExceptionDetails { get; set; }

}

// Class to contain list of blob files info
public class BlobFileInfo
{
    public string FileName { get; set; }
    public string BlobPath { get; set; }
    public string BlobFilePath { get; set; }
    public IListBlobItem Blob { get; set; }
}
public class StaggingFinancialTransSummary
{
    public StaggingFinancialTransSummary() { }
    public int Id { get; set; }
    public int LogId { get; set; }
    public string ContractID { get; set; }
    public string SegmentID { get; set; }
    public int FinancialYear { get; set; }
    public decimal TotalValueExclGST { get; set; }
    public decimal TotalValueInclGST { get; set; }
    public decimal GST { get; set; }
}

public static class ErrorInfo
{
    public const string ErrorCode01 = "E001";
    public const string ErrorDescription01 = "Contract ID is a not null";

    public const string ErrorCode02 = "E002";
    public const string ErrorDescription02 = "Segment ID is a not null";

    public const string ErrorCode03 = "E003";
    public const string ErrorDescription03 = "Contract ID And Segment ID combination is valid combination";

    public const string ErrorCode04 = "E004";
    public const string ErrorDescription04 = "Financial year is in the format YYYY";

    public const string ErrorCode05 = "E005";
    public const string ErrorDescription05 = "Total Value Excl. GST is not null";

    public const string ErrorCode06 = "E006";
    public const string ErrorDescription06 = "Total Value Excl. GST is a float";

    public const string ErrorCode07 = "E007";
    public const string ErrorDescription07 = "Total Value incl. GST is not null";

    public const string ErrorCode08 = "E008";
    public const string ErrorDescription08 = "Total Value incl. GST is a float";

    public const string ErrorCode09 = "E009";
    public const string ErrorDescription09 = "GST is a float or GST is null";

    public const string ErrorCode10 = "E010";
    public const string ErrorDescription10 = "Incorrect number of values";

    public const string ErrorCode11 = "E011";
    public const string ErrorDescription11 = "Duplicate data. Contract ID, Segment ID and Financial year must unique values";
    public static string ErrorCode { get; set; }
    public static string ErrorDescription { get; set; }
    public static int NumberOfTimesFailed { get; set; }
}
public enum enumErrorCode
{
    E000,
    [Description("E001")]
    E001 = 1,
    [Description("E002")]
    E002 = 2,
    [Description("E003")]
    E003 = 3,
    [Description("E004")]
    E004,
    [Description("E005")]
    E005,
    [Description("E006")]
    E006,
    [Description("E007")]
    E007,
    [Description("E008")]
    E008,
    [Description("E009")]
    E009,
    [Description("E010")]
    E010,
    [Description("E011")]
    E011
};

public class BlobHelper
{
    public BlobHelper()
    {
        SendGridAPIKey = "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";
        SummaryValidation = new ArrayList();
        DetailsValidation = new ArrayList();
    }

    //For params
    public string FileNamePattern { get; set; }
    public string BlobFileName { get; set; }
    // File setting
    public string CurrentDirectory { get; set; }
    public string ExceptionDetails { get; set; }
    public string RootDirectory { get; set; }
    public string CurrentFileNameFullPath { get; set; }
    public string CurrentFileName { get; set; }
    public string CurrentFileNameWithoutExtension { get; set; }
    public string Separator { get; set; }
    public string TextQualifier { get; set; }
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

    // for CloudStorage
    public string CloudStorageAccountName { get; set; }
    public string CloudStorageAccountKey { get; set; }
    public string UploadResult { get; set; }
    public string CloudStorageConnection { get; set; }
    public string CloudBlobContainerName { get; set; }
    public string TempCloudStoragePath { get; set; }
    public string CloudStorageConnectionString { get; set; }

    // for logging
    public DateTime CompletedDatetime { get; set; }
    public DateTime StartDatetime { get; set; }
    public int NumberOfFile { get; set; }
    public string LoadStatus { get; set; }
    public string LogName { get; set; }

    public int LogId { get; set; }

    //for SmtpClient
    public string MessageEmail { get; set; }
    public string ReceiverEmail { get; set; }
    public string SendGridAPIKey { get; set; }

    //for current Integrate
    public string InvalidDetails { get; set; }
    public string InvalidSummary { get; set; }
    public string IntegrateSatus { get; set; }
    public int NumberOfRowsInFile { get; set; }
    //Contracts, Segment
    Dictionary<string, string> PRG_ContractsAndSegmentNumber = new Dictionary<string, string>();

    public ArrayList SummaryValidation;

    public ArrayList DetailsValidation;
    public string Base64Encode(string plainText)
    {
        var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
        return System.Convert.ToBase64String(plainTextBytes);
    }

    private bool CheckPropertyExist(dynamic obj, string propertyName, System.Collections.ArrayList arrPropertyExist)
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
        StartDatetime = DateTime.Now;
        RootDirectory = "D:\\home\\site\\wwwroot\\";  //Update late

        try
        { 

            //Inital for SqlAzure  : GenericWebhookCSharpLoadCsvToSqlAzure
            LogName = "GenericWebhookCSharpLoadCsvToSqlAzure";
            if (CheckPropertyExist(data, "IntSqlUserID", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.IntSqlUserID.ToString()))
                    arrMissing.Add("IntSqlUserID");
                else
                    IntSqlUserID= data.IntSqlUserID.ToString();
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

            //Inital for CloudStorage
            CurrentDirectory = RootDirectory + LogName;
            TempCloudStoragePath = CurrentDirectory + "\\TempStorage";

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

            //Inital for csv
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

            if (CheckPropertyExist(data, "BlobFileName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.BlobFileName.ToString()))
                    arrMissing.Add("BlobFileName");
                else
                    BlobFileName = data.BlobFileName.ToString();
            }


            //Inital for SMTP          
            if (CheckPropertyExist(data, "MessageEmail", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.MessageEmail.ToString()))
                    arrMissing.Add("MessageEmail");
                else
                    MessageEmail = data.MessageEmail.ToString();
            }

            if (CheckPropertyExist(data, "ReceiverEmail", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.ReceiverEmail.ToString()))
                    arrMissing.Add("ReceiverEmail");
                else
                    ReceiverEmail = data.ReceiverEmail.ToString();
            }


            //for Integrate Data
            ExceptionDetails = string.Empty;
            IntegrateSatus = "Completed";

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
    public SendGrid.Helpers.Mail.Attachment GetAttachment(string strContent, string strFileName)
    {
        string ContentBase64String = Base64Encode(strContent);

        SendGrid.Helpers.Mail.Attachment att = new SendGrid.Helpers.Mail.Attachment();
        att.Type = "text/csv";
        att.Content = ContentBase64String;
        att.Filename = strFileName;
        att.Disposition = "attachment";
        att.ContentId = strFileName;
        return att;
    }
    public async Task SendEmailBySendGrid(string fileName, int totalErrors)
    {
        string summaryFileName = CurrentFileNameWithoutExtension + "-validation-summary.csv";
        string detailsFileName = CurrentFileNameWithoutExtension + "-validation-results.csv";
        SendGrid.Helpers.Mail.Attachment attSummaryFile = GetAttachment(InvalidSummary, summaryFileName);
        SendGrid.Helpers.Mail.Attachment attDetailsFile = GetAttachment(InvalidDetails, detailsFileName);

        StringBuilder strContent = new StringBuilder();
        Personalization personalization = new Personalization();
        SendGrid.Helpers.Mail.Email to;

        string[] ReceiverList = ReceiverEmail.Split(';');
        foreach (string sReceiver in ReceiverList)
        {
            to = new SendGrid.Helpers.Mail.Email(sReceiver);
            personalization.AddTo(to);
        }
        dynamic sg = new SendGrid.SendGridAPIClient(SendGridAPIKey);
        SendGrid.Helpers.Mail.Content content = new SendGrid.Helpers.Mail.Content();
        content.Type = "text/plain";
        string EOL = "\n";
        string Tab = "\t";
        strContent.AppendLine(string.Format("The file {0} failed validation and wasn’t loaded into Progenitor.", CurrentFileName));
        strContent.AppendLine(EOL + "The attached files:");
        strContent.AppendLine(EOL + Tab + string.Format("{0} contains a summary of the errors", summaryFileName));
        strContent.AppendLine(EOL + Tab + string.Format("{0} contains the first 1000 errors by row.", detailsFileName));
        strContent.AppendLine(EOL + Tab);
        content.Value = strContent.ToString();
        //define Mail Content
        string strSubject = string.Format("Validation Errors for file {0}", CurrentFileName);

        SendGrid.Helpers.Mail.Email from = new SendGrid.Helpers.Mail.Email(MessageEmail);
        //SendGrid.Helpers.Mail.Email to = new SendGrid.Helpers.Mail.Email(ReceiverEmail);        
        //SendGrid.Helpers.Mail.Mail mail = new SendGrid.Helpers.Mail.Mail(from, strSubject, to, content);
        SendGrid.Helpers.Mail.Mail mail = new SendGrid.Helpers.Mail.Mail();
        mail.From = from;
        mail.Subject = strSubject;
        mail.AddPersonalization(personalization);
        mail.AddContent(content);
        mail.AddAttachment(attSummaryFile);
        mail.AddAttachment(attDetailsFile);

        dynamic response = await sg.client.mail.send.post(requestBody: mail.Get());
    }


    /// <summary>
    /// Check FileName correct with pattern format
    /// </summary>
    /// <param name="filename"></param>
    /// <returns>bool value</returns>
    private bool CheckFileNameFormat(string filename)
    {
        if (System.Text.RegularExpressions.Regex.IsMatch(filename, FileNamePattern))
            return true;
        else
            return false;
    }

    private Microsoft.WindowsAzure.Storage.Auth.StorageCredentials GetStorageCredentials
    {
        get
        {
            Microsoft.WindowsAzure.Storage.Auth.StorageCredentials credentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(CloudStorageAccountName, CloudStorageAccountKey);
            return credentials;
        }
    }
    private SqlConnection GetSqlConnection
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

    private SqlConnection GetSqlConnectionOfIntSql
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
    public bool CheckAzureSqlConnection()
    {
        try
        {
            //Create a SqlConnection from the provided connection string
            using (SqlConnection connection = GetSqlConnection)
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

        // Create the blob client.
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

        // Retrieve a reference to a container.
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

    // Load blob container
    public CloudBlobContainer GetBlobContainer()
    {
        var storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(GetStorageCredentials, true);    //CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
        var blobClient = storageAccount.CreateCloudBlobClient();
        var container = blobClient.GetContainerReference(CloudBlobContainerName);
        return container;
    }

    //List the blobs in a container
    public List<BlobFileInfo> GetListBlobs()
    {
        // Retrieve reference to a previously created container.
        CloudBlobContainer container = GetBlobContainer();
        var blobInfos = new List<BlobFileInfo>();
        // Loop over items within the container and output the length and URI.
        foreach (IListBlobItem item in container.ListBlobs(null, false))
        {
            if (item.GetType() == typeof(CloudBlockBlob))
            {
                CloudBlockBlob blob = (CloudBlockBlob)item;

                var blobFileName = blob.Uri.Segments.Last().Replace("%20", " ");
                var blobFilePath = blob.Uri.AbsolutePath.Replace(blob.Container.Uri.AbsolutePath + "/", "").Replace("%20", " ");
                var blobPath = blobFilePath.Replace("/" + blobFileName, "");

                if (CheckFileNameFormat(blob.Name))
                {
                    blobInfos.Add(new BlobFileInfo
                    {
                        FileName = blob.Name,
                        BlobFilePath = blob.StorageUri.PrimaryUri.AbsolutePath.Replace(blob.Container.Uri.AbsolutePath + "/", "").Replace("%20", " "),
                        BlobPath = blobFilePath.Replace("/" + blob.StorageUri.PrimaryUri.Segments.Last().Replace("%20", " "), ""),
                        Blob = blob
                    });

                }

            }
        }
        return blobInfos;
    }

    // Get recursive list of files
    public IEnumerable<BlobFileInfo> ListFolderBlobs(string directoryName)
    {
        var blobContainer = GetBlobContainer();
        var blobDirectory = blobContainer.GetDirectoryReference(directoryName);
        var blobInfos = new List<BlobFileInfo>();
        var blobs = blobDirectory.ListBlobs().ToList();
        foreach (var blob in blobs)
        {
            if (blob is CloudBlockBlob)
            {
                var blobFileName = blob.Uri.Segments.Last().Replace("%20", " ");
                var blobFilePath = blob.Uri.AbsolutePath.Replace(blob.Container.Uri.AbsolutePath + "/", "").Replace("%20", " ");
                var blobPath = blobFilePath.Replace("/" + blobFileName, "");
                blobInfos.Add(new BlobFileInfo
                {
                    FileName = blobFileName,
                    BlobPath = blobPath,
                    BlobFilePath = blobFilePath,
                    Blob = blob
                });
            }
        }
        return blobInfos;
    }

    public bool GetFileFromCloudStorage(string strFileName)
    {
        //CurrentFileNameFullPath = strFileName;
        CurrentFileNameFullPath = TempCloudStoragePath + "\\" + strFileName;
        var blobContainer = GetBlobContainer();
        Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(strFileName);

        if (blockBlob.Exists())
        {
            try
            {
                // Save blob contents to a file.
                using (var fileStream = System.IO.File.OpenWrite(CurrentFileNameFullPath))
                {
                    blockBlob.DownloadToStream(fileStream);
                }
                CurrentFileName = strFileName;
                CurrentFileNameWithoutExtension = System.IO.Path.GetFileNameWithoutExtension(CurrentFileNameFullPath);
                return true;
            }
            catch (Microsoft.WindowsAzure.StorageClient.StorageClientException exception)
            {
                ExceptionDetails = exception.Message.ToString();
                return false;
            }
        }
        else
        {
            ExceptionDetails = "BlockBlob \"" + strFileName + "\" doesn't exists!";
            return false;
        }
    }

    public void Merging()
    {
        int iNumberOfRowEffect = 0;
        string strCMD = string.Empty;
        string errorLine = string.Empty;
        try
        {
            using (SqlConnection connection = GetSqlConnection)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.Text;
                strCMD += "MERGE[dbo].[FinancialTransSummary] AS TARGET  ";
                strCMD += "USING[dbo].[StaggingFinancialTransSummary] AS SOURCE  ";
                strCMD += "ON(  ";
                strCMD += "        TARGET.[ContractID] = SOURCE.[ContractID] ";
                strCMD += "        and TARGET.[SegmentID] = SOURCE.[SegmentID] ";
                strCMD += "        and TARGET.[FinancialYear] = SOURCE.[FinancialYear]  ";
                strCMD += "    )  ";
                strCMD += "WHEN MATCHED AND TARGET.[TotalValueExclGST] != SOURCE.[TotalValueExclGST] OR TARGET.[TotalValueInclGST] != SOURCE.[TotalValueInclGST] OR TARGET.[GST] != SOURCE.[GST] THEN  ";
                strCMD += "    UPDATE SET	[TotalValueExclGST] = SOURCE.[TotalValueExclGST], [TotalValueInclGST] = SOURCE.[TotalValueInclGST], [GST] = SOURCE.[GST], [IsDeleted] = 0 ";
                strCMD += "WHEN NOT MATCHED BY TARGET THEN ";
                strCMD += "    INSERT([LogId],[ContractID], [SegmentID], [FinancialYear], [TotalValueExclGST], [TotalValueInclGST], [GST], [IsDeleted])  ";
                strCMD += "    VALUES(SOURCE.[LogId], SOURCE.[ContractID], SOURCE.[SegmentID], SOURCE.[FinancialYear], SOURCE.[TotalValueExclGST], SOURCE.[TotalValueInclGST], SOURCE.[GST], 0)  ";
                strCMD += "WHEN NOT MATCHED BY SOURCE  THEN ";
                strCMD += "UPDATE SET [IsDeleted] = 1 ";
                strCMD += "; ";

                command.CommandText = strCMD;
                connection.Open();
                iNumberOfRowEffect = command.ExecuteNonQuery();
                connection.Dispose();
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
        }
    }

    public bool LoadingData(string strCurrentFileProcess)
    {
        CurrentFileName = strCurrentFileProcess;
        CurrentFileNameFullPath = TempCloudStoragePath + "\\" + strCurrentFileProcess;// full file path
        CurrentFileNameWithoutExtension = System.IO.Path.GetFileNameWithoutExtension(CurrentFileNameFullPath);
        int lineNumber = 0;
        System.IO.StreamReader reader = System.IO.File.OpenText(CurrentFileNameFullPath);
        var csv = new CsvHelper.CsvReader(reader);
        csv.Configuration.HasHeaderRecord = false;
        csv.Configuration.Delimiter = Separator;

        List<StaggingFinancialTransSummary> listObj = new List<StaggingFinancialTransSummary>();
        StaggingFinancialTransSummary obj;
        decimal fGST, fTotalValueExclGST, fTotalValueInclGST;
        string strContractsAndSegmentNumber = string.Empty;
        string strNumber;
        string strNullValue = "Null Value";
        ArrayList errorList = new ArrayList();

        //Write Log first for get LogId, will update lated.
        WriteIntegrateDataLogging();
        while (csv.Read())
        {
            lineNumber += 1;
            if (csv.CurrentRecord.Length == 6)
            {
                if (String.IsNullOrEmpty(csv.GetField<string>(0)))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E001) + Separator + lineNumber.ToString() + Separator + strNullValue);
                    //hasErrors = true;
                }

                if (String.IsNullOrEmpty(csv.GetField<string>(1)))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E002) + Separator + lineNumber.ToString() + Separator + strNullValue);
                    //hasErrors = true;
                }

                //Check Error 0003 
                strNumber = string.Empty;
                strContractsAndSegmentNumber = csv.GetField<string>(0) + csv.GetField<string>(1);
                if (!PRG_ContractsAndSegmentNumber.TryGetValue(strContractsAndSegmentNumber, out strNumber))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E003) + Separator + lineNumber.ToString() + Separator + "ContractsId = " + csv.GetField<string>(0) + " And SegmentNumber = " + csv.GetField<string>(1));
                    //hasErrors = true;
                }
                DateTime date;
                if (!DateTime.TryParse(string.Format("1/1/{0}", csv.GetField<string>(2)), out date))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E004) + Separator + lineNumber.ToString() + Separator + (String.IsNullOrEmpty(csv.GetField<string>(2))? strNullValue : csv.GetField<string>(2)));
                    //hasErrors = true;
                }
                if (String.IsNullOrEmpty(csv.GetField<string>(3)))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E005) + Separator + lineNumber.ToString() + Separator + strNullValue);
                    //hasErrors = true;
                }
                else if (!decimal.TryParse(csv.GetField<string>(3), out fTotalValueExclGST))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E006) + Separator + lineNumber.ToString() + Separator + csv.GetField<string>(3));
                    //hasErrors = true;
                }

                if (String.IsNullOrEmpty(csv.GetField<string>(4)))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E007) + Separator + lineNumber.ToString() + Separator + strNullValue);
                    //hasErrors = true;
                }
                else if (!decimal.TryParse(csv.GetField<string>(4), out fTotalValueInclGST))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E008) + Separator + lineNumber.ToString() + Separator + csv.GetField<string>(4));
                    //hasErrors = true;
                }

                if (String.IsNullOrEmpty(csv.GetField<string>(5)) || !decimal.TryParse(csv.GetField<string>(5), out fGST))
                {
                    errorList.Add(Convert.ToString((int)enumErrorCode.E009) + Separator + lineNumber.ToString() + Separator + (String.IsNullOrEmpty(csv.GetField<string>(5)) ? strNullValue : csv.GetField<string>(5)));
                    //hasErrors = true;
                }
            }
            else
            {
                errorList.Add(Convert.ToString((int)enumErrorCode.E010) + Separator + lineNumber.ToString() + Separator + "Wrong Format");

            }

            obj = new StaggingFinancialTransSummary();
            decimal.TryParse(csv.GetField<string>(3), out fTotalValueExclGST);
            decimal.TryParse(csv.GetField<string>(4), out fTotalValueInclGST);
            decimal.TryParse(csv.GetField<string>(5), out fGST);
            obj.ContractID = csv.GetField<string>(0);
            obj.SegmentID = csv.GetField<string>(1);
            obj.FinancialYear = csv.GetField<int>(2);
            obj.TotalValueExclGST = fTotalValueExclGST;
            obj.TotalValueInclGST = fTotalValueInclGST;
            obj.LogId = LogId;
            obj.GST = fGST;

            listObj.Add(obj);

            //lineNumber += 1;
            obj.Id = lineNumber;
        }
        NumberOfRowsInFile = lineNumber;
        // Dispose all reader
        reader.Dispose();
        csv.Dispose();

        var objBulk = new BulkUploadToSql<StaggingFinancialTransSummary>()
        {
            InternalStore = listObj,
            TableName = "StaggingFinancialTransSummary",
            CommitBatchSize = 1000,
            ConnectionString = GetSqlConnection.ConnectionString
        };
        objBulk.Commit();

        //For Error Code E011
        ArrayList errorCodeE011 = CheckDuplicateData();
        if (errorCodeE011.Count > 0)
        {
            foreach (string e11 in errorCodeE011)
            {
                errorList.Add(Convert.ToString((int)enumErrorCode.E011) + Separator + e11.ToString());
            }
        }


        //Check error exist
        if (errorList.Count > 0)
        {
            IntegrateSatus = "False";
            //objResult.SummaryOfValidationErrors = objIntegrate.InvalidSummary;
            DetailsValidation = errorList;
            GenerateErrorLogFile();
            ExceptionDetails = "False in validating data, occur " + DetailsValidation.Count.ToString() + " errors in this file. Error details :" + InvalidSummary.Replace("\"", "");
            SendEmailBySendGrid(CurrentFileName, DetailsValidation.Count).Wait();
            return false;
        }
        else
            Merging();
        //Truncate Stagging for next file;            
        return true;
    }

    public void TruncateStagging()
    {
        string strCMD = string.Empty;
        using (SqlConnection connection = GetSqlConnection)
        {
            SqlCommand command = new SqlCommand();
            command.Connection = connection;
            command.CommandTimeout = 10;
            command.CommandType = System.Data.CommandType.Text;
            strCMD += "truncate table dbo.[StaggingFinancialTransSummary]";
            command.CommandText = strCMD;
            connection.Open();
            command.ExecuteNonQuery();
            connection.Dispose();
        }
    }

    public void GetFullPRG_ContractsAndSegmentNumber()
    {
        string strCMD = "SELECT Distinct C.[PRG_ContractNumber],CS.[PRG_ConSegmentNumber] FROM DBO.PRG_Contracts C INNER JOIN DBO.PRG_Contract_Segments CS ON C.ListItemId = CS.[PRG_ConSegmentContract] WHERE CS.[PRG_ConSegmentNumber] IS NOT NULL AND LEFT(CS.[PRG_ConSegmentNumber], 1) != 'S'";
        string strContractsAndSegmentNumber = string.Empty;

        //Create a SqlConnection from the provided connection string
        using (SqlConnection connection = GetSqlConnectionOfIntSql)
        {
            SqlCommand command = new SqlCommand();
            command.Connection = connection;
            command.CommandTimeout = 10;
            command.CommandType = System.Data.CommandType.Text;
            command.CommandText = strCMD;
            connection.Open();
            SqlDataReader sqlReader = command.ExecuteReader();
            while (sqlReader.Read())
            {
                strContractsAndSegmentNumber = sqlReader[0].ToString() + sqlReader[1].ToString();
                PRG_ContractsAndSegmentNumber.Add(strContractsAndSegmentNumber, strContractsAndSegmentNumber);
            }
            connection.Dispose();
        }
    }
    public ArrayList CheckErrorCodeE003()
    {
        ArrayList errorList = new ArrayList();
        string strCMD = string.Empty;
        string errorLine = string.Empty;

        //Create a SqlConnection from the provided connection string
        using (SqlConnection connection = GetSqlConnection)
        {
            SqlCommand command = new SqlCommand();
            command.Connection = connection;
            command.CommandTimeout = 10;
            command.CommandType = System.Data.CommandType.Text;
            strCMD += "SELECT S.ID ";
            strCMD += "FROM    StaggingFinancialTransSummary AS s ";
            strCMD += "WHERE   NOT EXISTS ";
            strCMD += "      (SELECT  cs.PRG_ConSegmentNumber, cs.PRG_ConSegmentContract ";
            strCMD += "      FROM        PRG_Contract_Segments cs ";
            strCMD += "      WHERE(s.PRG_ContractsID = cs.PRG_ConSegmentContract) AND(s.SegmentID = cs.PRG_ConSegmentNumber))  ";

            command.CommandText = strCMD;
            connection.Open();
            SqlDataReader sqlReader = command.ExecuteReader();
            while (sqlReader.Read())
            {
                errorLine = sqlReader[0].ToString();
                errorList.Add(Convert.ToString((int)enumErrorCode.E003) + Separator + errorLine);
            }
            connection.Dispose();
            return errorList;
        }
    }

    public ArrayList CheckDuplicateData()
    {
        ArrayList arrDuplicateLine = new ArrayList();
        string strCMD = string.Empty;
        using (SqlConnection connection = GetSqlConnection)
        {
            SqlCommand command = new SqlCommand();
            command.Connection = connection;
            command.CommandTimeout = 10;
            command.CommandType = System.Data.CommandType.Text;
            strCMD += "With dup as (Select Id, rn = row_number() ";
            strCMD += "     Over(PARTITION BY[ContractID], [SegmentID],[FinancialYear] order by[ContractID],[SegmentID]) ";
            strCMD += "     From[dbo].[StaggingFinancialTransSummary]) ";
            strCMD += "Select Id From dup Where rn > 1";
            command.CommandText = strCMD;
            connection.Open();
            SqlDataReader sqlReader = command.ExecuteReader();
            while (sqlReader.Read())
            {
                arrDuplicateLine.Add(sqlReader[0].ToString());
            }
            connection.Dispose();
            return arrDuplicateLine;
        }
    }

    public int WriteIntegrateDataLogging()
    {
        string queryString = string.Empty;
        try
        {
            using (SqlConnection connection = GetSqlConnection)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.StoredProcedure;
                command.CommandText = "dbo.uspLogCSVToSqlAzureCreate";

                command.Parameters.Add("@LogId", System.Data.SqlDbType.Int).Direction = System.Data.ParameterDirection.Output;

                command.Parameters.Add("@FileName", System.Data.SqlDbType.NVarChar);
                command.Parameters["@FileName"].Value = CurrentFileName;

                command.Parameters.Add("@ContainerName", System.Data.SqlDbType.NVarChar);
                command.Parameters["@ContainerName"].Value = CloudBlobContainerName;

                command.Parameters.Add("@BlobStorageAccount", System.Data.SqlDbType.NVarChar);
                command.Parameters["@BlobStorageAccount"].Value = CloudStorageAccountName;

                command.Parameters.Add("@CompletedDatetime", System.Data.SqlDbType.DateTime);
                command.Parameters["@CompletedDatetime"].Value = DateTime.Now;

                command.Parameters.Add("@StartDatetime", System.Data.SqlDbType.DateTime);
                command.Parameters["@StartDatetime"].Value = StartDatetime;

                command.Parameters.Add("@IntegrateSatus", System.Data.SqlDbType.NVarChar);
                command.Parameters["@IntegrateSatus"].Value = IntegrateSatus;

                command.Parameters.Add("@ExceptionDetails", System.Data.SqlDbType.NVarChar);
                command.Parameters["@ExceptionDetails"].Value = ExceptionDetails;
                connection.Open();
                command.ExecuteNonQuery();

                // read output value from @NewId
                LogId = Convert.ToInt32(command.Parameters["@LogId"].Value);
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return 0;
        }
        return LogId;
    }

    public void UpdateIntegrateDataLogging()
    {
        string queryString = string.Empty;

        queryString = "Update[dbo].LogCSVToSqlAzure ";
        queryString += "Set[IntegrateSatus] = '" + IntegrateSatus + "', ";
        queryString += "[ExceptionDetails] = '" + ExceptionDetails + "' ";
        queryString += "Where LogId = " + LogId.ToString();
        try
        {
            using (SqlConnection connection = GetSqlConnection)
            {
                SqlCommand command = new SqlCommand();
                command.Connection = connection;
                command.CommandTimeout = 10;
                command.CommandType = System.Data.CommandType.Text;
                command.CommandText = queryString;
                connection.Open();
                command.ExecuteNonQuery();
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
        }
    }

    public bool GenerateErrorLogFile()
    {
        int iErrorCode = 0;
        int iNumberOfE001 = 0;
        int iNumberOfE002 = 0;
        int iNumberOfE003 = 0;
        int iNumberOfE004 = 0;
        int iNumberOfE005 = 0;
        int iNumberOfE006 = 0;
        int iNumberOfE007 = 0;
        int iNumberOfE008 = 0;
        int iNumberOfE009 = 0;
        int iNumberOfE010 = 0;
        int iNumberOfE011 = 0;
        string strline = string.Empty;
        string strErrorCode = string.Empty;
        string strErrorOnLine = string.Empty;
        string strWrongValue = string.Empty;
        string strSeparator = ",";
        string strValidationSummaryPath = CurrentFileNameFullPath.Replace(".csv", string.Empty) + "-validation-summary.csv";
        string strValidationDetailPath = CurrentFileNameFullPath.Replace(".csv", string.Empty) + "-validation-results.csv";
        System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();

        try
        {
            strBuilder.Clear();
            strline = string.Empty;
            strErrorCode = string.Empty;
            strErrorOnLine = string.Empty;

            // Generate DetailsValidation Summary Validation report.                
            //Add Headers
            strBuilder.AppendLine(TextQualifier + "Error code" + TextQualifier + strSeparator + TextQualifier + "Row Number" + TextQualifier + strSeparator + TextQualifier + "Wrong Value" + TextQualifier);
            //Add Details
            foreach (string str in DetailsValidation)
            {
                string[] part = str.Split('|');//value = "1|1" -- error code = enum | row;

                enumErrorCode eErrorCode = (enumErrorCode)Enum.Parse(typeof(enumErrorCode), part[0].ToString(), true);
                strErrorCode = eErrorCode.ToString();
                strErrorOnLine = part[1].ToString();
                iErrorCode = Convert.ToInt32(part[0].ToString());
                strWrongValue = part[2].ToString();

                strBuilder.AppendLine(TextQualifier + strErrorCode + TextQualifier + strSeparator + strErrorOnLine + strSeparator + TextQualifier + strWrongValue + TextQualifier );
                // Count Error
                switch (iErrorCode)
                {
                    case (int)enumErrorCode.E001:
                        strErrorCode = enumErrorCode.E001.ToString();
                        iNumberOfE001 += 1;
                        break;
                    case (int)enumErrorCode.E002:
                        iNumberOfE002 += 1;
                        break;
                    case (int)enumErrorCode.E003:
                        iNumberOfE003 += 1;
                        break;
                    case (int)enumErrorCode.E004:
                        iNumberOfE004 += 1;
                        break;
                    case (int)enumErrorCode.E005:
                        iNumberOfE005 += 1;
                        break;
                    case (int)enumErrorCode.E006:
                        iNumberOfE006 += 1;
                        break;
                    case (int)enumErrorCode.E007:
                        iNumberOfE007 += 1;
                        break;
                    case (int)enumErrorCode.E008:
                        iNumberOfE008 += 1;
                        break;
                    case (int)enumErrorCode.E009:
                        iNumberOfE009 += 1;
                        break;
                    case (int)enumErrorCode.E010:
                        iNumberOfE010 += 1;
                        break;
                    case (int)enumErrorCode.E011:
                        iNumberOfE011 += 1;
                        break;
                }
            }
            InvalidDetails = strBuilder.ToString();

            // Generate Summary Validation report.                
            strBuilder.Clear();
            //Add Headers
            strBuilder.AppendLine(TextQualifier + "Error code" + TextQualifier + strSeparator + TextQualifier + "Error description" + TextQualifier + strSeparator + TextQualifier + "Number of times failed" + TextQualifier);
            //Add Details
            if (iNumberOfE001 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E001.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription01 + TextQualifier + strSeparator + iNumberOfE001.ToString());
            if (iNumberOfE002 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E002.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription02 + TextQualifier + strSeparator + iNumberOfE002.ToString());
            if (iNumberOfE003 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E003.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription03 + TextQualifier + strSeparator + iNumberOfE003.ToString());
            if (iNumberOfE004 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E004.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription04 + TextQualifier + strSeparator + iNumberOfE004.ToString());
            if (iNumberOfE005 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E005.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription05 + TextQualifier + strSeparator + iNumberOfE005.ToString());
            if (iNumberOfE006 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E006.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription06 + TextQualifier + strSeparator + iNumberOfE006.ToString());
            if (iNumberOfE007 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E007.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription07 + TextQualifier + strSeparator + iNumberOfE007.ToString());
            if (iNumberOfE008 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E008.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription08 + TextQualifier + strSeparator + iNumberOfE008.ToString());
            if (iNumberOfE009 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E009.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription09 + TextQualifier + strSeparator + iNumberOfE009.ToString());
            if (iNumberOfE010 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E010.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription10 + TextQualifier + strSeparator + iNumberOfE010.ToString());
            if (iNumberOfE011 > 0)
                strBuilder.AppendLine(TextQualifier + enumErrorCode.E011.ToString() + TextQualifier + strSeparator + TextQualifier + ErrorInfo.ErrorDescription11 + TextQualifier + strSeparator + iNumberOfE011.ToString());

            InvalidSummary = strBuilder.ToString();
            return true;
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
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
