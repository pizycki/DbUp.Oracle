using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;

namespace DbUp.Oracle
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a Oracle database
    /// </summary>
    public class OracleTableJournal : IJournal
    {
        private readonly IConnectionManager _connectionManager;
        private readonly IUpgradeLog _log;

        /// <summary>
        ///
        /// </summary>
        protected string JournalTableName { get; } // Dont't make table name uppercase. It's not required.

        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableJournal"/> class.
        /// </summary>
        /// <param name="connectionManager">The connection manager.</param>
        /// <param name="logger">The log.</param>
        /// <param name="table">The table name.</param>
        public OracleTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string table)
        {
            _connectionManager = connectionManager();
            _log = logger();

            JournalTableName = table;
        }

        /// <summary>
        /// Recalls the version number of the database.
        /// </summary>
        /// <returns>All executed scripts.</returns>
        public IEnumerable<string> GetExecutedScripts()
        {
            _log.WriteInformation("Fetching list of already executed scripts.");

            var exists = DoesJournalTableExists();
            if (!exists)
            {
                _log.WriteInformation($"The { JournalTableName } table could not be found. The database is assumed to be at version 0.");
                return Enumerable.Empty<string>();
            }

            var scripts = new List<string>();
            _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = $@"SELECT SCRIPT_NAME
                                             FROM { JournalTableName }
                                             WHERE FAILURE_STATEMENT_INDEX IS NULL
                                               AND FAILURE_REMARK IS NULL
                                             ORDER BY SCRIPT_NAME ";

                    using (var reader = command.ExecuteReader())
                        while (reader.Read())
                            scripts.Add((string)reader[0]);

                }
            });

            return scripts.ToArray();
        }

        string[] IJournal.GetExecutedScripts() => GetExecutedScripts().ToArray();

        /// <summary>
        /// Validate already executed SqlScript with state in database.
        /// </summary>
        /// <param name="script">SqlScript to validate.</param>
        /// <returns>True if SqlScript is valid.</returns>
        public bool ValidateScript(SqlScript script)
        {
            return ValidateExecutedScript(script, null);
        }

        /// <summary>
        /// Get index of failed part in oracle script from journal database table.
        /// </summary>
        /// <param name="script">Script to get index for failed part.</param>
        /// <returns>Index of failed part</returns>
        public virtual int GetFailedStatementIndex(SqlScript script)
        {
            var exists = DoesJournalTableExists();

            if (!exists)
            {
                return 0;
            }

            return _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = $@"SELECT FAILURE_STATEMENT_INDEX
                                             FROM {JournalTableName}
                                             WHERE SCRIPT_NAME = :scriptName";

                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = "scriptName";
                    scriptNameParam.Value = script.Name;
                    command.Parameters.Add(scriptNameParam);

                    using (var reader = command.ExecuteReader())
                        while (reader.Read())
                            return Convert.ToInt32(reader[0]);
                }

                // When nothing was readed
                return 0;
            });
        }

        /// <summary>
        /// Get hash of sucessfully executed parts of failed oracle script from journal database table.
        /// This is intendant for validation of already successfully executed parts of Oracle scripts, so developers don't change allready appiled changes.
        /// </summary>
        /// <param name="script">Script to get hash of sucessfully executed parts.</param>
        /// <returns>Index of failed part</returns>
        public int GetFailedStatementHash(SqlScript script)
        {
            var exists = DoesJournalTableExists();

            if (!exists)
            {
                return 0;
            }

            return _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = $@"SELECT SCRIPT_HASHCODE
                                             FROM {JournalTableName}
                                             WHERE SCRIPT_NAME = :scriptName";

                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = "scriptName";
                    scriptNameParam.Value = script.Name;
                    command.Parameters.Add(scriptNameParam); ;

                    using (var reader = command.ExecuteReader())
                        while (reader.Read())
                            return Convert.ToInt32(reader[0]);
                }

                // When nothing was readed
                return 0;
            });
        }

        /// <summary>
        /// Records a database upgrade for a database specified in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="failureStatementIndex">Statments that were successfully executed. If null, all statements in script has been successfully executed. </param>
        /// <param name="failureRemark"/>
        public void StoreExecutedScript(SqlScript script, int? failureStatementIndex, string failureRemark)
        {
            var successfullStatments = _connectionManager.SplitScriptIntoCommands(script.Contents);

            if (failureStatementIndex.HasValue)
            {
                successfullStatments = successfullStatments.Take(failureStatementIndex.Value);
            }

            var journalExists = DoesJournalTableExists();
            _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                if (journalExists == false)
                {
                    CreateJournalTableInDatabase();
                }
                else
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = $@"DELETE FROM {JournalTableName}
                                                 WHERE SCRIPT_NAME = :scriptName
                                                   AND FAILURE_STATEMENT_INDEX IS NOT NULL
                                                   AND FAILURE_REMARK IS NOT NULL";

                        var scriptNameParam = command.CreateParameter();
                        scriptNameParam.ParameterName = "scriptName";
                        scriptNameParam.Value = script.Name;
                        command.Parameters.Add(scriptNameParam);

                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                    }
                }


                using (var command = dbCommandFactory())
                {
                    command.CommandText =
                        $@"INSERT INTO {JournalTableName}
                           (
                                SCRIPT_NAME,
                                APPLIED,
                                FAILURE_STATEMENT_INDEX,
                                FAILURE_REMARK,
                                SCRIPT_HASHCODE
                           )
                           VALUES
                           (
                                :scriptName,
                                TO_DATE(:applied, 'yyyy-mm-dd hh24:mi:ss'),
                                :failureStatementIndex,
                                :failureRemark,
                                :hash
                            )";

                    // SCRIPT_NAME
                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = "scriptName";
                    scriptNameParam.Value = script.Name;
                    command.Parameters.Add(scriptNameParam);

                    // APPLIED
                    var appliedParam = command.CreateParameter();
                    appliedParam.ParameterName = "applied";
                    appliedParam.Value = $"{DateTime.UtcNow:yyyy-MM-dd hh:mm:ss}";
                    command.Parameters.Add(appliedParam);

                    // FAILURE_STATEMENT_INDEX
                    var successfullStatementIndexParam = command.CreateParameter();
                    successfullStatementIndexParam.ParameterName = "failureStatementIndex";
                    successfullStatementIndexParam.Value = failureStatementIndex;
                    command.Parameters.Add(successfullStatementIndexParam);

                    // FAILURE_REMARK
                    var failureRemarkParam = command.CreateParameter();
                    failureRemarkParam.ParameterName = "failureRemark";
                    failureRemarkParam.Value = failureRemark;
                    command.Parameters.Add(failureRemarkParam);

                    // SCRIPT_HASHCODE
                    var hashParam = command.CreateParameter();
                    hashParam.ParameterName = "hash";
                    hashParam.Value = CalculateHash(successfullStatments);
                    command.Parameters.Add(hashParam);

                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            });
        }

        /// <summary>
        /// Check if already executed part of script has changed.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="successfullyExecutedStatements">Collection of already successfull executed statements of scipt</param>
        /// <returns>Return true if already executed statements of scripts have not changed.</returns>
        public bool ValidateExecutedScript(SqlScript script, IEnumerable<string> successfullyExecutedStatements)
        {
            if (successfullyExecutedStatements == null)
            {
                successfullyExecutedStatements = _connectionManager.SplitScriptIntoCommands(script.Contents);
            }

            var successfullHash = GetFailedStatementHash(script);
            var scriptsHash = CalculateHash(successfullyExecutedStatements);
            return successfullHash == scriptsHash;
        }

        public void StoreExecutedScript(SqlScript script)
        {
            var exists = DoesJournalTableExists();
            if (!exists)
            {
                CreateJournalTableInDatabase();
            }

            _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    command.CommandText = $"INSERT INTO { JournalTableName } (SCRIPT_NAME, APPLIED) VALUES (:scriptName, :applied)";

                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = ":scriptName";
                    scriptNameParam.Value = script.Name;
                    command.Parameters.Add(scriptNameParam);

                    var appliedParam = command.CreateParameter();
                    appliedParam.ParameterName = ":applied";
                    appliedParam.Value = DateTime.UtcNow;
                    command.Parameters.Add(appliedParam);

                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            });
        }

        /// <summary>
        ///
        /// </summary>
        protected virtual void CreateJournalTableInDatabase()
        {
            _log.WriteInformation($"Creating the {JournalTableName} table");

            _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                using (var command = dbCommandFactory())
                {
                    command.CommandText = CreateTableSql();
                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }

                _log.WriteInformation($"The {JournalTableName} table has been created");
            });
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        protected string CreateTableSql()
        {
            var command = $@"CREATE TABLE { JournalTableName }
                             (
                                ID integer GENERATED ALWAYS AS IDENTITY(start with 1 increment by 1 nocycle),
                                SCRIPT_NAME VARCHAR2(255) NOT NULL,
                                APPLIED TIMESTAMP NOT NULL,
                                REMARK VARCHAR2(4000) NULL,
                                FAILURE_STATEMENT_INDEX integer NULL,
                                FAILURE_REMARK VARCHAR2(4000) NULL,
                                SCRIPT_HASHCODE integer NULL,
                                CONSTRAINT PK_{ JournalTableName } PRIMARY KEY (ID) ENABLE VALIDATE
                             )";

            return command;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        private static int CalculateHash(IEnumerable<string> collection)
        {
            if (collection == null || !collection.Any()) return 0;
            return collection.Aggregate(0, (current, entry) => current ^ entry.GetHashCode());
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        protected virtual bool DoesJournalTableExists()
        {
            return _connectionManager.ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                try
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandType = CommandType.Text;
                        command.CommandText = $@"SELECT TABLE_NAME
                                                 FROM USER_TABLES
                                                 WHERE TABLE_NAME='{ JournalTableName }'";

                        using (var reader = command.ExecuteReader())
                        {
                            // Read only first row in result
                            // If there is any returned row, then table exists in schema
                            var rowExists = reader.Read();
                            return rowExists;
                        }
                    }
                }
                catch (SqlException)
                {
                    return false;
                }
                catch (DbException)
                {
                    return false;
                }
            });
        }

        public const string DefaultJournalName = "schemaversions";
    }
}