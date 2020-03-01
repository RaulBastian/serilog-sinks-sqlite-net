// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.SQLLiteNET;
using SQLite;

namespace Serilog.Sinks.Extensions
{
    internal class SQLiteSinkNET : BatchProvider, ILogEventSink
    {

        private readonly string _databasePath;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly uint _maxDatabaseSize;
        private readonly bool _rollOver;
        private readonly string _tableName;
        private readonly TimeSpan? _retentionPeriod;
        private readonly Timer _retentionTimer;
        private const long BytesPerMb = 1_048_576;
        private const long MaxSupportedPages = 5_242_880;
        private const long MaxSupportedPageSize = 4096;
        private const long MaxSupportedDatabaseSize = unchecked(MaxSupportedPageSize * MaxSupportedPages) / 1048576;
        private static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        public SQLiteSinkNET(
            string sqlLiteDbPath,
            string tableName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            TimeSpan? retentionPeriod,
            TimeSpan? retentionCheckInterval,
            uint batchSize = 100,
            uint maxDatabaseSize = 10,
            bool rollOver = true) : base(batchSize: (int)batchSize, maxBufferSize: 100_000)
        {
            _databasePath = sqlLiteDbPath;
            _tableName = tableName;
            _formatProvider = formatProvider;
            _storeTimestampInUtc = storeTimestampInUtc;
            _maxDatabaseSize = maxDatabaseSize;
            _rollOver = rollOver;

            if (maxDatabaseSize > MaxSupportedDatabaseSize)
            {
                throw SQLiteException.New(SQLite3.Result.Error, $"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
            }

            InitializeDatabase();

            if (retentionPeriod.HasValue)
            {
                // impose a min retention period of 15 minute
                var retentionCheckMinutes = 15;
                if (retentionCheckInterval.HasValue)
                {
                    retentionCheckMinutes = Math.Max(retentionCheckMinutes, retentionCheckInterval.Value.Minutes);
                }

                // impose multiple of 15 minute interval
                retentionCheckMinutes = (retentionCheckMinutes / 15) * 15;

                _retentionPeriod = new[] { retentionPeriod, TimeSpan.FromMinutes(30) }.Max();

                // check for retention at this interval - or use retentionPeriod if not specified
                _retentionTimer = new Timer(
                    (x) => { ApplyRetentionPolicy(); },
                    null,
                    TimeSpan.FromMinutes(0),
                    TimeSpan.FromMinutes(retentionCheckMinutes));
            }
        }

        #region ILogEvent implementation

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private void InitializeDatabase()
        {
            using (var conn = GetSqLiteConnection())
            {
                CreateSqlTable(conn);
            }
        }

        private SQLiteConnection GetSqLiteConnection()
        {
            return new SQLiteConnection(_databasePath, false); ;
        }

        private void CreateSqlTable(SQLiteConnection sqlConnection)
        {
            var colDefs = "id INTEGER PRIMARY KEY AUTOINCREMENT,";
            colDefs += "Timestamp TEXT,";
            colDefs += "Level VARCHAR(10),";
            colDefs += "Exception TEXT,";
            colDefs += "RenderedMessage TEXT,";
            colDefs += "Properties TEXT";

            var sqlCreateText = $"CREATE TABLE IF NOT EXISTS {_tableName} ({colDefs})";

            var sqlCommand = new SQLiteCommand(sqlConnection);
            sqlCommand.CommandText = sqlCreateText;
            sqlCommand.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlInsertCommand(SQLiteConnection connection)
        {
            var sqlInsertText = "INSERT INTO {0} (Timestamp, Level, Exception, RenderedMessage, Properties)";
            sqlInsertText += " VALUES (@timeStamp, @level, @exception, @renderedMessage, @properties)";
            sqlInsertText = string.Format(sqlInsertText, _tableName);

            var sqlCommand = new SQLiteCommand(connection);
            sqlCommand.CommandText = sqlInsertText;
           
            return sqlCommand;
        }

        private void ApplyRetentionPolicy()
        {
            var epoch = DateTimeOffset.Now.Subtract(_retentionPeriod.Value);
            using (var sqlConnection = GetSqLiteConnection())
            {
                var cmd = CreateSqlDeleteCommand(sqlConnection, epoch);

                SelfLog.WriteLine("Deleting log entries older than {0}", epoch);
                var ret = cmd.ExecuteNonQuery();
                SelfLog.WriteLine($"{ret} records deleted");

            }
        }

        private void TruncateLog(SQLiteConnection sqlConnection)
        {
            var cmd = new SQLiteCommand(sqlConnection);
            cmd.CommandText = $"DELETE FROM {_tableName}";
            cmd.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlDeleteCommand(SQLiteConnection sqlConnection, DateTimeOffset epoch)
        {
            var cmd = new SQLiteCommand(sqlConnection);
            cmd.CommandText = $"DELETE FROM {_tableName} WHERE Timestamp < @epoch";
            cmd.Bind("@epoch", (_storeTimestampInUtc ? epoch.ToUniversalTime() : epoch).ToString("yyyy-MM-ddTHH:mm:ss"));
            return cmd;
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return true;
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                using (var sqlConnection = GetSqLiteConnection())
                {
                    try
                    {
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);
                        return true;
                    }
                    catch (SQLiteException e)
                    {
                        SelfLog.WriteLine(e.Message);


                        if (e.Result != SQLite3.Result.Full)
                            return false;

                        if (_rollOver == false)
                        {
                            SelfLog.WriteLine("Discarding log excessive of max database");

                            return true;
                        }

                        var dbExtension = Path.GetExtension(_databasePath);

                        var newFilePath = Path.Combine(Path.GetDirectoryName(_databasePath) ?? "Logs",
                            $"{Path.GetFileNameWithoutExtension(_databasePath)}-{DateTime.Now:yyyyMMdd_hhmmss.ff}{dbExtension}");

                        File.Copy(_databasePath, newFilePath, true);

                        TruncateLog(sqlConnection);
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                        //var result = sqlConnection.Query<EventDTO>("select * from Logs")?.Count();

                        SelfLog.WriteLine($"Rolling database to {newFilePath}");
                        return true;
                    }
                    catch (Exception e)
                    {
                        SelfLog.WriteLine(e.Message);
                        return false;
                    }
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private async Task WriteToDatabaseAsync(ICollection<LogEvent> logEventsBatch, SQLiteConnection sqlConnection)
        {
            await Task.Factory.StartNew(() =>
            {
                try
                {
                    sqlConnection.Trace = true;
                    sqlConnection.BeginTransaction();

                    var sqlCommand = CreateSqlInsertCommand(sqlConnection);

                    foreach (var logEvent in logEventsBatch)
                    {
                        sqlCommand.Bind("@timeStamp", _storeTimestampInUtc ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") : logEvent.Timestamp.ToString("yyyy-MM-ddTHH:mm:ss"));
                        sqlCommand.Bind("@level", logEvent.Level.ToString());
                        sqlCommand.Bind("@exception", logEvent.Exception?.ToString() ?? string.Empty);
                        sqlCommand.Bind("@renderedMessage", logEvent.MessageTemplate.Text);
                        sqlCommand.Bind("@properties", logEvent.Properties.Count > 0 ? logEvent.Properties.Json() : string.Empty);
                        sqlCommand.ExecuteNonQuery();
                    }

                    sqlConnection.Commit();
                }
                catch (Exception ex)
                {

                    throw;
                }
            }).ConfigureAwait(false);
        }
    }
}
