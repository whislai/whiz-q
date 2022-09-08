using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Transactions;

using IsolationLevel = System.Transactions.IsolationLevel;

namespace WhizQ
{
    public sealed class SqlFactory
    {
        public static ISqlDatabase SqlDatabase { get; private set; }
        public static bool DisableTransactionScope { get; set; } = false;
        public static IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;
        public static bool AutoCommit { get; set; } = true;
        public static bool EnableTextFileLogging { get; set; } = false;

        private static readonly string[] ReservedWords = { "using", "namespace", "public", "private", "internal", "protected", "virtual", "sealed",
            "abstract", "class", "interface", "get", "set", "return", "checked", "default", "event" };

        public static void Configure(DatabaseProvider databaseProvider, string connectionString)
        {
            SqlDatabase = new SqlDatabase(databaseProvider, connectionString);
        }

        public static void Configure(ISqlConfig sqlConfig)
        {
            SqlDatabase = new SqlDatabase(sqlConfig.DatabaseProvider, sqlConfig.ConnectionString);
        }

        public static void Configure(ISqlDatabase sqlDatabase)
        {
            SqlDatabase = sqlDatabase;
        }

        private static List<ISql> QRun(List<ISql> sqlList, bool toSave = false)
        {
            using (var transactionScope = (!toSave || DisableTransactionScope || !AutoCommit || sqlList.Any(x => x.SqlCommand == SqlCommand.Truncate)) ? null :
                new TransactionScope(TransactionScopeOption.Required, new TransactionOptions() { IsolationLevel = IsolationLevel }))
            {
                for (int i = 0; i < sqlList.Count; i++)
                {
                    dynamic sql = sqlList[i];
                    sql.SqlDatabase = sql.GetSqlDatabase();
                }
                var sqlDatabases = sqlList.Select(x => new { x.SqlDatabase.DatabaseProvider, x.SqlDatabase.ConnectionString }).Distinct();
                var iSqlDatabases = sqlDatabases.Select(x => new SqlDatabase(x.DatabaseProvider, x.ConnectionString) as ISqlDatabase).ToList();

                for (int i = 0; i < iSqlDatabases.Count; i++)
                {
                    var iSqlList = sqlList.Where(x => x.SqlDatabase.DatabaseProvider == iSqlDatabases[i].DatabaseProvider &&
                    x.SqlDatabase.ConnectionString == iSqlDatabases[i].ConnectionString).ToList();
                    using (IDbConnection connection = iSqlDatabases[i].Open())
                    {
                        for (int j = 0; j < iSqlList.Count; j++)
                        {
                            dynamic sql = iSqlList[j];
                            sql.Count = 0;
                            sql.Result = 0;
                            sql.SqlError = "";
                            sql.Errors = new List<Error>();
                            if (toSave)
                            {
                                if (sql.SqlCommand == SqlCommand.Skip || sql.SqlCommand == SqlCommand.Select)
                                {
                                    sql.SqlCommand = SqlCommand.Replace;
                                }
                                if (!DisableTransactionScope && (!AutoCommit || sql.SqlCommand == SqlCommand.Truncate) && Transaction.Current == null)
                                {
                                    sql.SqlError = "Transaction scope must be provided for manual commit";
                                }
                                else
                                {
                                    sql.RunSaveSQL(connection);
                                }
                            }
                            else
                            {
                                sql.SqlCommand = SqlCommand.Select;
                                sql.RunSelectSQL(connection);
                            }
                            sql.ResolveSQL();
                            if (iSqlList[j].Errors.Count > 0)
                            {
                                break;
                            }
                        }
                    }
                }

                if (transactionScope != null &&
                    sqlList.All(x => string.IsNullOrEmpty(x.SqlError)) &&
                    sqlList.Select(x => x.Count).Sum() == sqlList.Select(x => x.Result).Sum() &&
                    sqlList.Select(x => x.Errors.Count).Sum() == 0)
                {
                    transactionScope.Complete();
                }
            }
            return sqlList;
        }

        public static List<ISql> Select(List<ISql> sqlList)
        {
            return QRun(sqlList);
        }

        public static List<ISql> Save(List<ISql> sqlList)
        {
            return QRun(sqlList, true);
        }

        public static void GenerateCSFiles(ISqlDatabase sqlDatabase, string classNamespace, string path, bool keyNullable = true)
        {
            if (sqlDatabase.DatabaseProvider == DatabaseProvider.PostgreSQL || sqlDatabase.DatabaseProvider == DatabaseProvider.SQLite3)
            {
                return;
            }
            string databaseName = sqlDatabase.ConnectionString.ToLower().Split("database=")[1].Split(";")[0].Trim();
            string tablesName = "";
            string columnsName = "";
            string schemaColumnName = "";
            string boolTypeName = "";
            string[] decimalTypeNames = { };
            if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
            {
                tablesName = "information_schema.tables";
                columnsName = "information_schema.columns";
                schemaColumnName = "TABLE_CATALOG";
                boolTypeName = "smallint";
                decimalTypeNames = new[] { "numeric", "decimal" };
            }
            else if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL)
            {
                tablesName = "information_schema.tables";
                columnsName = "information_schema.columns";
                schemaColumnName = "TABLE_SCHEMA";
                boolTypeName = "tinyint";
                decimalTypeNames = new[] { "decimal" };
            }
            var tablesQ = new Sql<dynamic>(tablesName).Configure(sqlDatabase).IncludeColumn("TABLE_NAME")
                .And("TABLE_TYPE", "=", "BASE TABLE").And(schemaColumnName, "=", databaseName);
            var tables = tablesQ.Select().List;
            if (tables.Count > 0)
            {
                var columnsQ = new Sql<dynamic>(columnsName).Configure(sqlDatabase).IncludeColumns(columnsName, new[] { "TABLE_NAME", "COLUMN_NAME", "IS_NULLABLE" })
                    .And(schemaColumnName, "=", databaseName);
                if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                {
                    var keyColumnsQ = new Sql<dynamic>("information_schema.key_column_usage").Configure(sqlDatabase).IncludeColumn("CONSTRAINT_NAME")
                        .And(schemaColumnName, "=", databaseName);
                    columnsQ = columnsQ.IncludeColumn("DATA_TYPE").LeftJoin(keyColumnsQ, "COLUMN_NAME").Or("CONSTRAINT_NAME", "LIKE", "PK")
                        .Or("CONSTRAINT_NAME", "IS", null);
                }
                else if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL)
                {
                    columnsQ = columnsQ.IncludeColumns(new[] { "COLUMN_TYPE", "COLUMN_KEY" });
                }
                var columns = columnsQ.OrderBy(columnsName, "ORDINAL_POSITION").Select().List;
                for (int t = 0; t < tables.Count; t++)
                {
                    bool anyDateTimeColumn = false;
                    var tColumns = new List<dynamic>();
                    for (int j = 0; j < columns.Count; j++)
                    {
                        if (columns[j].TABLE_NAME == tables[t].TABLE_NAME)
                        {
                            tColumns.Add(columns[j]);
                        }
                    }
                    string tableName = tables[t].TABLE_NAME;
                    var classFileName = tableName + ".cs";
                    var sb = new StringBuilder();
                    sb.Append("namespace " + classNamespace + Environment.NewLine);
                    sb.Append("{" + Environment.NewLine);
                    sb.Append("    public class ");
                    if (ReservedWords.Any(x => x.Equals(tableName)))
                    {
                        tableName = tableName.Substring(0, 1).ToUpper() + tableName.Substring(1).ToLower();
                    }
                    sb.Append(tableName + Environment.NewLine + "    {" + Environment.NewLine);
                    for (int c = 0; c < tColumns.Count; c++)
                    {
                        string columnName = tColumns[c].COLUMN_NAME;
                        string typeColumnName = "";
                        if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                        {
                            typeColumnName = tColumns[c].DATA_TYPE;
                        }
                        else if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL)
                        {
                            typeColumnName = tColumns[c].COLUMN_TYPE;
                        }
                        var isString = false;
                        sb.Append("        public ");
                        if (typeColumnName.StartsWith(boolTypeName))
                        {
                            sb.Append("bool");
                        }
                        else if (typeColumnName.StartsWith("int"))
                        {
                            sb.Append("int");
                        }
                        else if (typeColumnName.StartsWith("bigint"))
                        {
                            sb.Append("long");
                        }
                        else if (typeColumnName.StartsWith("double"))
                        {
                            sb.Append("double");
                        }
                        else if (typeColumnName.StartsWith("float"))
                        {
                            sb.Append("float");
                        }
                        else if (decimalTypeNames.Any(x => typeColumnName.StartsWith(x)))
                        {
                            sb.Append("decimal");
                        }
                        else if (typeColumnName.Contains("char") || typeColumnName.Contains("text"))
                        {
                            isString = true;
                            sb.Append("string");
                        }
                        else if (typeColumnName.StartsWith("date") || typeColumnName.StartsWith("time"))
                        {
                            anyDateTimeColumn = true;
                            sb.Append("DateTime");
                        }
                        if (ReservedWords.Any(x => x.Equals(columnName)) || columnName.Equals(tableName))
                        {
                            if (columnName.Substring(0, 1).ToUpper().Equals(tableName.Substring(0, 1).ToUpper()))
                            {
                                columnName = "_" + columnName;
                            }
                            else
                            {
                                columnName = columnName.Substring(0, 1).ToUpper() + columnName.Substring(1).ToLower();
                            }
                        }
                        bool setNullableKey = false;
                        if (keyNullable)
                        {
                            if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                            {
                                if (tColumns[c].CONSTRAINT_NAME != null && tColumns[c].CONSTRAINT_NAME.StartsWith("PK"))
                                {
                                    setNullableKey = true;
                                }
                            }
                            else if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                            {
                                if (tColumns[c].COLUMN_KEY == "PRI")
                                {
                                    setNullableKey = true;
                                }
                            }
                        }
                        if (!isString && (tColumns[c].IS_NULLABLE == "YES" || setNullableKey))
                        {
                            sb.Append("? ");
                        }
                        else
                        {
                            sb.Append(" ");
                        }
                        sb.Append(columnName + " { get; set; }" + Environment.NewLine);
                    }
                    sb.Append("    }" + Environment.NewLine);
                    sb.Append("}");
                    var usingSystem = "";
                    if (anyDateTimeColumn)
                    {
                        usingSystem = "using System;" + Environment.NewLine + Environment.NewLine;
                    }
                    File.WriteAllText(path + "/" + classFileName, usingSystem + sb.ToString());
                }
            }
            else
            {
                if (EnableTextFileLogging)
                {
                    Api.WriteLog(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + " [" + Api.GetMethodName() + "] No table found");
                }
                else
                {
                    throw new Exception("No table found");
                }
            }
        }
    }
}
