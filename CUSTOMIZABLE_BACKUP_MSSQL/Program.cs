using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.Data.SqlClient;

class Program
{
    static async Task Main()
    {
        string executablePath = AppDomain.CurrentDomain.BaseDirectory;
        string pathFilePath = Path.Combine(executablePath, "path.txt");
        string databaseFilePath = Path.Combine(executablePath, "database.txt");
        string[] paths;
        string path1 = string.Empty, path2 = string.Empty;
        string logFilePath = string.Empty;

        // Read and parse the path file
        try
        {
            if (File.Exists(pathFilePath))
            {
                paths = File.ReadAllLines(pathFilePath);
                if (paths.Length > 0)
                {
                    path1 = paths[0];
                    logFilePath = Path.Combine(path1, "Log.txt");

                    if (paths.Length > 1)
                    {
                        path2 = paths[1];
                    }
                }
                else
                {
                    Log("Path file is empty. No backup paths provided.", logFilePath);
                    return;
                }
            }
            else
            {
                Log("Path file is missing. Cannot proceed.", logFilePath);
                return;
            }
        }
        catch (Exception ex)
        {
            Log($"Error reading path file: {ex.Message}", logFilePath);
            return;
        }

        // Read and parse the database file
        string[] databases;
        try
        {
            if (File.Exists(databaseFilePath))
            {
                databases = File.ReadAllLines(databaseFilePath);
                if (databases.Length == 0)
                {
                    Log("Database file is empty. Backup all databases.", logFilePath);
                    databases = GetAllDatabases();
                }
            }
            else
            {
                Log("Database file is missing. Backup all databases.", logFilePath);
                databases = GetAllDatabases();
            }
        }
        catch (Exception ex)
        {
            Log($"Error reading database file: {ex.Message}", logFilePath);
            return;
        }

        // Task list to manage backup and copy operations
        Task previousCopyTask = Task.CompletedTask;

        // Backup each database and handle copying simultaneously
        foreach (var database in databases)
        {
            if (string.IsNullOrWhiteSpace(database)) continue;

            // Wait for the previous copy task to complete before starting the next backup
            await previousCopyTask;

            try
            {
                string backupFile = await BackupDatabaseWithProgress(database.Trim(), path1, logFilePath);

                // Start copying the backup file to path2 while the next backup begins
                if (!string.IsNullOrEmpty(path2))
                {
                    previousCopyTask = CopyBackupFile(backupFile, path2, logFilePath);
                }
            }
            catch (Exception ex)
            {
                Log($"Error backing up database {database}: {ex.Message}", logFilePath);
            }
        }

        // Ensure the last copy task completes
        await previousCopyTask;
    }

    // Dynamic method to get all databases from the server
    static string[] GetAllDatabases()
    {
        string connectionString = "Server=(LOCAL); Integrated Security=True; Encrypt=True; TrustServerCertificate=True;";
        List<string> databases = new List<string>();

        string query = @"
            SELECT name 
            FROM sys.databases 
            WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
            AND state = 0"; // state = 0 means the database is online

        try
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlCommand command = new SqlCommand(query, connection);
                command.CommandTimeout = 120; // Set a higher timeout for fetching databases
                connection.Open();

                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        databases.Add(reader.GetString(0));
                    }
                }
            }
            Log("Successfully retrieved all databases from the server.", Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Log.txt"));
        }
        catch (Exception ex)
        {
            Log($"Error retrieving databases: {ex.Message}", Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Log.txt"));
        }

        return databases.ToArray();
    }

    // Method to backup a database to a specified path with progress reporting
    static async Task<string> BackupDatabaseWithProgress(string databaseName, string backupPath, string logFilePath)
    {
        string backupFileName = $"{databaseName}_{DateTime.Now:yyyyMMddHHmmss}.bak";
        string backupFilePath = Path.Combine(backupPath, backupFileName);
        string connectionString = "Server=localhost; Integrated Security=True; Encrypt=True; TrustServerCertificate=True;";

        // Use Microsoft.Data.SqlClient.SqlConnection for SMO
        using (var sqlConnection = new SqlConnection(connectionString))
        {
            ServerConnection connection = new ServerConnection(sqlConnection);
            Server sqlServer = new Server(connection);

            try
            {
                Backup backup = new Backup
                {
                    Action = BackupActionType.Database,
                    Database = databaseName,
                    Initialize = true,
                    BackupSetName = $"{databaseName} Backup",
                    BackupSetDescription = $"Backup of {databaseName}",
                    Incremental = false,
                };

                backup.Devices.AddDevice(backupFilePath, DeviceType.File);
                backup.PercentCompleteNotification = 10; // Set notification interval
                backup.PercentComplete += (sender, e) =>
                {
                    string progressMessage = $"Backing up {databaseName}: {e.Percent}% completed.";
                    Log(progressMessage, logFilePath);
                    Console.WriteLine(progressMessage);
                };

                await Task.Run(() => backup.SqlBackup(sqlServer));
                Log($"Successfully backed up database {databaseName} to {backupFilePath}", logFilePath);
            }
            catch (Exception ex)
            {
                Log($"Error backing up database {databaseName}: {ex.Message}", logFilePath);
            }
        }

        return backupFilePath;
    }

    // Method to copy a backup file to a secondary path
    static async Task CopyBackupFile(string sourceFilePath, string destinationPath, string logFilePath)
    {
        string fileName = Path.GetFileName(sourceFilePath);
        string destinationFilePath = Path.Combine(destinationPath, fileName);

        try
        {
            using (FileStream sourceStream = new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read))
            using (FileStream destinationStream = new FileStream(destinationFilePath, FileMode.Create, FileAccess.Write))
            {
                await sourceStream.CopyToAsync(destinationStream);
            }
            Log($"Successfully copied {sourceFilePath} to {destinationFilePath}", logFilePath);
        }
        catch (Exception ex)
        {
            Log($"Error copying {sourceFilePath} to {destinationFilePath}: {ex.Message}", logFilePath);
        }
    }

    // Method to log messages to a file
    static void Log(string message, string logFilePath)
    {
        string logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}";
        Console.WriteLine(logMessage);
        if (!string.IsNullOrEmpty(logFilePath))
        {
            try
            {
                File.AppendAllText(logFilePath, logMessage + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to log message to file: {ex.Message}");
            }
        }
    }
}
