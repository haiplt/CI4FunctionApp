//GenericWebhookCSharpAzureBlobToSFTP

/*
{
      "CloudBlobContainerName": "contracts-segments",
      "CloudStorageAccountKey": "ofwlZWH1hrnhGFdaULHRKfXDhKYwfTueUT9P3yE+kX0TcZ7rKQ7Xand/sWMArODwIPvkcrhN5ejcYTND2B+67g==",
      "CloudStorageAccountName": "devprogendataintstorage",
      "SftpHostName": "104.210.108.35",
      "SftpUserName": "tim",
      "SftpPassword": "gJe8gUXMAj7KWWf4qzzF",
      "SftpSshHostKeyFingerprint": "ssh-ed25519 256 4a:56:51:ba:ff:f8:75:12:44:d0:5d:16:66:0f:d7:96",
      "SftpSshPrivateKeyFile": "tim_private_key.ppk",
      "SftpPrivateKeyPassphrase": "gJe8gUXMAj7KWWf4qzzF",
      "SftpDestinationFolder": "//outgoing/contracts//contract-segment-list//",
      "FileName": "contracts-segments-2016-11-07T072023Z.csv"
}
*/
#r "Newtonsoft.Json"
#r "WinSCPnet.dll"
#r "Microsoft.Azure.KeyVault.Core.dll"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "Microsoft.WindowsAzure.StorageClient.dll" 

using System;
using System.Net;
using System.Configuration;
using Newtonsoft.Json;
using Microsoft.Azure;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage;
using System.Collections.Generic;

public static async Task<object> Run(HttpRequestMessage req, TraceWriter log)
{
    log.Info($"---------- Function started  ----------------------------------------------------");
    log.Info($"GenericWebhookCSharpAzureBlobToSFTP was triggered!");

    string strUploadResult = string.Empty;
    string jsonContent = await req.Content.ReadAsStringAsync();
    dynamic data = JsonConvert.DeserializeObject(jsonContent);

    AzureBlobStorageToSFTP objUpload = new AzureBlobStorageToSFTP();

    if (!objUpload.InitVariable(data))
    {
        log.Info(objUpload.ExceptionDetails);
        return req.CreateResponse(HttpStatusCode.BadRequest, new
        {
            error = objUpload.ExceptionDetails
        });
    }
    else
    {
        //Initial log object            
        UploadResult objResult = new UploadResult();
        objResult.Filename = data.FileName.ToString();
        objResult.UploadStatus = true;
        objResult.Description = "GenericWebhookCSharpAzureBlobToSFTP";
        objResult.ExceptionDetail = string.Empty;

        Newtonsoft.Json.Linq.JObject jLogObject = (Newtonsoft.Json.Linq.JObject)Newtonsoft.Json.Linq.JToken.FromObject(objResult);
        log.Info("AzureBlobToSFTP after initial Variables");
        log.Info(jLogObject.ToString());
        //Full workflow for upload file : 
        log.Info("1.Check Connecting for SFTP, Azure Storage");
        if (objUpload.CheckSFTPConnection())
        {
            log.Info($" Check Connecting for SFTP : Connected");
            if (objUpload.CheckCloudStorageConnection())
            {
                log.Info($" Check Connecting for Azure Storage : Connected");

                //2. Get file from BlobStorage to TempStorage base on condition
                if (objUpload.GetFileFromCloudStorage())
                {
                    log.Info($"     Done Get File {objUpload.FileName} from CloudStorage");
                    //3. Put file TempStorage to SFTP
                    if (objUpload.UploadFileToSFTP(log))
                    {
                        //Delete Blobs file after upload to Azure Storage success
                        System.IO.File.Delete(objUpload.TempCloudStoragePath + "\\" + objUpload.FileName);
                        log.Info($"     Done upload to SFTP with file:  {objUpload.FileName}");
                    }
                    else
                    {
                        objResult.UploadStatus = false;
                        objResult.ExceptionDetail = objUpload.ExceptionDetails;
                        log.Info($" Can't Upload File To SFTP");
                        log.Info($" ExceptionDetail : {objUpload.ExceptionDetails}");
                    }
                }
                else
                {
                    objResult.UploadStatus = false;
                    objResult.ExceptionDetail = objUpload.ExceptionDetails;
                    log.Info($" Can't get file from CloudStorage");
                    log.Info($" ExceptionDetail : {objUpload.ExceptionDetails}");
                }
            }
            else
            {
                objResult.UploadStatus = false;
                objResult.ExceptionDetail = "Connecting for Azure Storage : Can't Connecting to Azure Storage";
                log.Info($"Check Connecting for Azure Storage : Can't Connecting to Azure Storage");
                log.Info($"Exception detail: {objUpload.ExceptionDetails}");
            }
        }
        else
        {
            objResult.UploadStatus = false;
            objResult.ExceptionDetail = "Connecting for SFTP : Can't Connecting to SFTP";
            log.Info($"Check Connecting for SFTP : Can't Connecting to SFTP");
        }

        log.Info($"4.Write log & return result with JSON");
        Newtonsoft.Json.Linq.JObject jObject = (Newtonsoft.Json.Linq.JObject)Newtonsoft.Json.Linq.JToken.FromObject(objResult);
        using (System.IO.StreamWriter file = System.IO.File.CreateText(objUpload.TempCloudStoragePath + "\\" + objUpload.UploadResult))
        using (Newtonsoft.Json.JsonTextWriter writer = new Newtonsoft.Json.JsonTextWriter(file))
        {
            jObject.WriteTo(writer);
        }
        strUploadResult = jObject.ToString();
        log.Info($"Export Result: {strUploadResult}");
        log.Info($"---------- Function completed ----------------------------------------------------");

        return req.CreateResponse(HttpStatusCode.OK, jObject);
    }

        
}

public class UploadResult
{
    public string Filename { get; set; }
    public bool UploadStatus { get; set; }
    public string Description { get; set; }
    public string ExceptionDetail { get; set; }

}


public class AzureBlobStorageToSFTP
{
    #region Declare Vaiable & Property
    public string ExceptionDetails { get; set; }
    public string RootDirectory { get; set; }
    public string CurrentDirectory { get; set; }
    public string FileName { get; set; }
    public string SftpFullName { get; set; }
    public string CloudStorageFullName { get; set; }
    // for SFTP
    public string SftpHostName { get; set; }
    public string SftpUserName { get; set; }
    public string SftpPassword { get; set; }
    public string SftpSshHostKeyFingerprint { get; set; }
    public string SftpSshPrivateKeyFile { get; set; }
    public string SftpPrivateKeyPassphrase { get; set; }
    public string SftpHomePath { get; set; }
    public string SftpDestinationFolder { get; set; }

    // for CloudStorage
    public string CloudStorageAccountName { get; set; }
    public string CloudStorageAccountKey { get; set; }
    public string UploadResult { get; set; }
    public string CloudStorageConnection { get; set; }
    public string CloudBlobContainerName { get; set; }
    public string TempCloudStoragePath { get; set; }
    public string CloudStorageConnectionString { get; set; }
    #endregion

    private WinSCP.SessionOptions SFTPSession
    {
        get
        {
            // Setup session options
            WinSCP.SessionOptions sessionOptions = new WinSCP.SessionOptions
            {
                Protocol = WinSCP.Protocol.Sftp,
                HostName = SftpHostName,
                UserName = SftpUserName,
                Password = SftpPassword,
                SshHostKeyFingerprint = SftpSshHostKeyFingerprint,
                SshPrivateKeyPath = @SftpSshPrivateKeyFile,
                PrivateKeyPassphrase = SftpPrivateKeyPassphrase
            };
            return sessionOptions;
        }
    }
    #region SFTP
    public bool CheckSFTPConnection()
    {
        using (WinSCP.Session session = new WinSCP.Session())
        {
            // Connect
            session.Open(SFTPSession);
            SftpHomePath = session.HomePath;
            return session.Opened;
        }
    }

    public bool CheckCloudStorageConnection()
    {
        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(CloudStorageAccountName, CloudStorageAccountKey), true); 

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

    public bool UploadFileToSFTP(TraceWriter log)
    {
        string localPath, remotePath;
        try
        {
            using (WinSCP.Session session = new WinSCP.Session())
            {
                // Connect
                session.Open(SFTPSession);
                log.Info(session.HomePath);
                ExceptionDetails = session.HomePath + SftpDestinationFolder + FileName;
                localPath = TempCloudStoragePath + "\\" + FileName;
                remotePath = session.HomePath + SftpDestinationFolder + FileName;

                WinSCP.TransferOptions transferOptions = new WinSCP.TransferOptions();
                transferOptions.TransferMode = WinSCP.TransferMode.Binary;

                WinSCP.TransferOperationResult transferResult;
                transferResult = session.PutFiles(localPath, remotePath, false, transferOptions);

                if (!transferResult.IsSuccess)
                {
                    foreach (WinSCP.TransferEventArgs transfer in transferResult.Transfers)
                    {
                        ExceptionDetails = transfer.Error.Message.ToString();
                    }
                }
                return transferResult.IsSuccess;
            }
        }
        catch (Exception ex)
        {
            ExceptionDetails = ex.ToString();
            return false;
        }
        
        

    }
    #endregion

    public bool GetFileFromCloudStorage()
    {
        string strFileName = TempCloudStoragePath + "\\" + FileName;
        Microsoft.WindowsAzure.Storage.CloudStorageAccount storageAccount = new Microsoft.WindowsAzure.Storage.CloudStorageAccount(new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(CloudStorageAccountName, CloudStorageAccountKey), true); 
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        Microsoft.WindowsAzure.Storage.Blob.CloudBlobContainer container = blobClient.GetContainerReference(CloudBlobContainerName);
        Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blockBlob = container.GetBlockBlobReference(FileName);

        if (blockBlob.Exists())
        {
            try
            {
                // Save blob contents to a file.
                using (var fileStream = System.IO.File.OpenWrite(strFileName))
                {
                    blockBlob.DownloadToStream(fileStream);
                }
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
            ExceptionDetails = "BlockBlob \"" + FileName + "\" doesn't exists!";
            return false;
        }
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
        //StartDatetime = DateTime.Now;
        RootDirectory = "D:\\home\\site\\wwwroot\\GenericWebhookCSharpAzureBlobToSFTP\\";  //Update late

        try
        { 

            //Inital 
            CurrentDirectory = RootDirectory;
            TempCloudStoragePath = CurrentDirectory + "TempStorage";
            UploadResult = "UploadResult.JSON";
            ExceptionDetails = string.Empty;

            if (CheckPropertyExist(data, "FileName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.FileName.ToString()))
                    arrMissing.Add("FileName");
                else
                    FileName = data.FileName.ToString();
            }

            if (CheckPropertyExist(data, "SftpHostName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpHostName.ToString()))
                    arrMissing.Add("SftpHostName");
                else
                    SftpHostName = data.SftpHostName.ToString();
            }

            if (CheckPropertyExist(data, "SftpUserName", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpUserName.ToString()))
                    arrMissing.Add("SftpUserName");
                else
                    SftpUserName = data.SftpUserName.ToString();
            }

            if (CheckPropertyExist(data, "SftpPassword", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpPassword.ToString()))
                    arrMissing.Add("SftpPassword");
                else
                    SftpPassword = data.SftpPassword.ToString();
            }

            if (CheckPropertyExist(data, "SftpSshHostKeyFingerprint", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpSshHostKeyFingerprint.ToString()))
                    arrMissing.Add("SftpSshHostKeyFingerprint");
                else
                    SftpSshHostKeyFingerprint = data.SftpSshHostKeyFingerprint.ToString();
            }

            if (CheckPropertyExist(data, "SftpSshPrivateKeyFile", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpSshPrivateKeyFile.ToString()))
                    arrMissing.Add("SftpSshPrivateKeyFile");
                else
                    SftpSshPrivateKeyFile = TempCloudStoragePath + "\\" + data.SftpSshPrivateKeyFile.ToString();
            }

            if (CheckPropertyExist(data, "SftpPrivateKeyPassphrase", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpPrivateKeyPassphrase.ToString()))
                    arrMissing.Add("SftpPrivateKeyPassphrase");
                else
                    SftpPrivateKeyPassphrase = data.SftpPrivateKeyPassphrase.ToString();
            }

            if (CheckPropertyExist(data, "SftpDestinationFolder", arrPropertyExist))
            {
                if (string.IsNullOrEmpty(data.SftpDestinationFolder.ToString()))
                    arrMissing.Add("SftpDestinationFolder");
                else
                    SftpDestinationFolder = data.SftpDestinationFolder.ToString();
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

