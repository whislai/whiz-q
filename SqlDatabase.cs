using System.Data;
using System.Data.SqlClient;
using System.Data.SQLite;

using MySql.Data.MySqlClient;
using Npgsql;

namespace WhizQ
{
    public class SqlDatabase : ISqlDatabase
    {
        public DatabaseProvider DatabaseProvider { get; set; }
        public string ConnectionString { get; set; }
        public IDbConnection Connection { get; set; }

        public SqlDatabase()
        {
            
        }

        public SqlDatabase(DatabaseProvider databaseProvider, string connectionString)
        {
            Configure(databaseProvider, connectionString);
        }

        public SqlDatabase(ISqlConfig sqlConfig)
        {
            Configure(sqlConfig);
        }

        public SqlDatabase(ISqlDatabase sqlDatabase)
        {
            Configure(sqlDatabase);
        }

        public void Configure(DatabaseProvider databaseProvider, string connectionString)
        {
            DatabaseProvider = databaseProvider;
            ConnectionString = connectionString;
        }

        public void Configure(ISqlConfig sqlConfig)
        {
            Configure(sqlConfig.DatabaseProvider, sqlConfig.ConnectionString);
        }

        public void Configure(ISqlDatabase sqlDatabase)
        {
            Configure(sqlDatabase.DatabaseProvider, sqlDatabase.ConnectionString);
        }

        public IDbConnection Open()
        {
            if (DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
            {
                Connection = new SqlConnection(ConnectionString);
            }
            else if (DatabaseProvider == DatabaseProvider.MySQL)
            {
                Connection = new MySqlConnection(ConnectionString);
            }
            else if (DatabaseProvider == DatabaseProvider.PostgreSQL)
            {
                Connection = new NpgsqlConnection(ConnectionString);
            }
            else if (DatabaseProvider == DatabaseProvider.SQLite3)
            {
                Connection = new SQLiteConnection(ConnectionString);
            }
            return Connection;
        }
    }
}
