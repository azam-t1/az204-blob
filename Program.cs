using System.Formats.Asn1;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

Console.WriteLine("Azure Blob Storage exercise\n");

// Run the examples asynchronously, wait for the results before proceeding
ProcessAsync().GetAwaiter().GetResult();

Console.WriteLine("Press enter to exit the sample application.");
Console.ReadLine();

static async Task ProcessAsync()
{
    // Copy the connection string from the portal in the variable below.
    string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=az204functionapp09262024;AccountKey=<key>;EndpointSuffix=core.windows.net";
    string _localPath = Path.Combine(Environment.CurrentDirectory, "files");
    string _directoryPath = Directory.Exists(_localPath) ? _localPath : Directory.CreateDirectory(_localPath).FullName;

    // Create a client that can authenticate with a connection string
    BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);

    // create container
    BlobContainerClient containerClient = await CreateContainer(blobServiceClient);

    // update container
    await UpdateBlobsToContainer(containerClient, _localPath);

    // list blobs
    await ListBlobs(containerClient, _directoryPath);

    // download blobs
    await DownloadBlobs(containerClient);

    // retrieves metadata from a container
    await ReadContainerMetadataAsync(containerClient);
}

static async Task<BlobContainerClient> CreateContainer(BlobServiceClient blobServiceClient)
{
    //Create a unique name for the container
    string containerName = "wtblob" + Guid.NewGuid().ToString();

    // Create the container and return a container client object
    BlobContainerClient containerClient = await blobServiceClient.CreateBlobContainerAsync(containerName);
    Console.WriteLine("A container named '" + containerName + "' has been created. " +
        "\nTake a minute and verify in the portal." + 
        "\nNext a file will be created and uploaded to the container.");
    Console.WriteLine("Press 'Enter' to continue.");
    Console.ReadLine();

    return containerClient;
}

static async Task UpdateBlobsToContainer(BlobContainerClient containerClient, string localPath)
{
    try
    {
        // Create the /data/ directory if it doesn't exist
        Directory.CreateDirectory(localPath);

        string fileName = "wtfile" + Guid.NewGuid().ToString() + ".txt";
        string localFilePath = Path.Combine(localPath, fileName);

        // Write text to the file
        await File.WriteAllTextAsync(localFilePath, "Hello, World!");

        // Get a reference to the blob
        BlobClient blobClient = containerClient.GetBlobClient(fileName);

        Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);

        // Open the file and upload its data
        using (FileStream uploadFileStream = File.OpenRead(localFilePath))
        {
            await blobClient.UploadAsync(uploadFileStream);
            uploadFileStream.Close();
        }

        Console.WriteLine("\nThe file was uploaded. We'll verify by listing the blobs next.");
        Console.WriteLine("Press 'Enter' to continue.");
        Console.ReadLine();

        return;    
    }
    catch (DirectoryNotFoundException ex)
    {
        // Handle the exception
        Console.WriteLine($"Error: {ex.Message}");
        // Provide alternative logic or fallback mechanisms
    }
    catch (Exception ex)
    {
        // Handle other exceptions
        Console.WriteLine($"Unexpected error: {ex.Message}");
    }
}

static async Task ListBlobs(BlobContainerClient containerClient, string _directoryPath)
{
    // List blobs in the container
    Console.WriteLine("Listing blobs...");
    await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
    {
        Console.WriteLine("\t" + blobItem.Name);
    }

    Console.WriteLine("\nYou can also verify by looking inside the " + 
            "container in the portal." +
            "\nNext the blob will be downloaded with an altered file name.");
    Console.WriteLine("Press 'Enter' to continue.");
    Console.ReadLine();

    return;
}

static async Task DownloadBlobs(BlobContainerClient containerClient)
{
    // Create a temporary directory for downloading blobs
    string tempDirectoryPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
    Directory.CreateDirectory(tempDirectoryPath);

    try
    {
        // List blobs in the container
        Console.WriteLine("Listing blobs...");
        await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
        {
            Console.WriteLine("\t" + blobItem.Name);

            // Get a reference to the blob
            BlobClient blobClient = containerClient.GetBlobClient(blobItem.Name);

            // Download the blob to a local file
            string downloadFilePath = Path.Combine(tempDirectoryPath, $"{blobItem.Name}_DOWNLOADED.txt");

            Console.WriteLine($"\nDownloading blob to\n\t{downloadFilePath}\n");

            // Download the blob's contents and save it to a file
            BlobDownloadInfo download = await blobClient.DownloadAsync();

            using (FileStream downloadFileStream = File.OpenWrite(downloadFilePath))
            {
                await download.Content.CopyToAsync(downloadFileStream);
            }

        }

        // copy from temp to persistent folder
        var persistentDirectoryPath = Path.Combine(Environment.CurrentDirectory, "files");
        Console.WriteLine($"\nCopying downloaded files to persistent directory: {persistentDirectoryPath}");

        // Copy downloaded files to the persistent directory
        foreach (string filePath in Directory.GetFiles(tempDirectoryPath))
        {
            string fileName = Path.GetFileName(filePath);
            string destinationFilePath = Path.Combine(persistentDirectoryPath, fileName);
            File.Copy(filePath, destinationFilePath, true);
        }

        Console.WriteLine($"\nLocate the downloaded files in the temporary directory: {tempDirectoryPath}");
        Console.WriteLine("The next step is to delete the container and local files.");
        Console.WriteLine("Press 'Enter' to continue.");
        Console.ReadLine();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error downloading blobs: {ex.Message}");
    }
    finally
    {
        // Delete the temporary directory and its contents
        Directory.Delete(tempDirectoryPath, true);
    }
}

static async Task ReadContainerMetadataAsync(BlobContainerClient container)
{
    try
    {
        var properties = await container.GetPropertiesAsync();

        // Enumerate the container's metadata.
        Console.WriteLine("Container metadata:");
        foreach (var metadataItem in properties.Value.Metadata)
        {
            Console.WriteLine($"\tKey: {metadataItem.Key}");
            Console.WriteLine($"\tValue: {metadataItem.Value}");
        }
    }
    catch (RequestFailedException e)
    {
        Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
        Console.WriteLine(e.Message);
        Console.ReadLine();
    }
}