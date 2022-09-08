using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Transactions;

using Dapper;

namespace WhizQ
{
    public class Sql<T> : ISql where T : new()
    {
        public ISqlDatabase SqlDatabase { get; internal set; }
        public bool? DisableTransactionScope { get; set; }
        public System.Transactions.IsolationLevel? IsolationLevel { get; set; }
        public bool? AutoCommit { get; set; }
        public SqlCommand SqlCommand { get; internal set; }
        public string RootTable { get; internal set; } = "";
        public List<Criteria> CriteriaList { get; private set; } = new List<Criteria>();
        public List<Criteria> RootFieldList { get; private set; } = new List<Criteria>();
        public List<Criteria> TableFieldList { get; internal set; } = new List<Criteria>();
        public DynamicParameters Parameters { get; private set; } = new DynamicParameters();
        public string SQL { get; private set; } = "";
        public int Count { get; internal set; }
        public int Result { get; internal set; }
        public string SqlError { get; internal set; } = "";
        public List<Error> Errors { get; internal set; } = new List<Error>();
        public T Data { get; internal set; }
        public List<T> List { get; internal set; } = new List<T>();
        public List<dynamic> DynamicList { get; internal set; } = new List<dynamic>();

        public Sql()
        {
            QSetData();
        }

        public Sql(DatabaseProvider databaseProvider, string connectionString)
        {
            SqlDatabase = new SqlDatabase(databaseProvider, connectionString);
            QSetData();
        }

        public Sql(ISqlConfig sqlConfig)
        {
            SqlDatabase = new SqlDatabase(sqlConfig);
            QSetData();
        }

        public Sql(ISqlDatabase sqlDatabase)
        {
            SqlDatabase = sqlDatabase;
            QSetData();
        }

        private void SetRootTable(dynamic data = null)
        {
            //Use custom `Table` name if provided, otherwise use argument/data generic type class name as table name 
            if (RootTable == "")
            {
                if (Api.IsGenericType(typeof(T)))
                {
                    RootTable = typeof(T).Name;
                }
                else if (Data != null && Api.IsGenericType(Data.GetType()))
                {
                    RootTable = Data.GetType().Name;
                }
                else if (data != null && Api.IsGenericType(data.GetType()))
                {
                    RootTable = data.GetType().Name;
                }
            }
        }

        private void QSetData(dynamic data = null)
        {
            //Data will be set based on generic type T argument
            //If another generic type data is provided via constructor or method parameter, it will be ignored
            if (Data == null && Api.IsGenericType(typeof(T)))
            {
                Data = new T();
            }
            if (data != null)
            {
                //Data will be replaced directly when input data has same generic type or Sql<T> is non-generic type, i.e. Anonymous/ExpandoObject
                if (Data == null && (Api.IsGenericType(data.GetType()) ||
                    (!Api.IsGenericType(data.GetType()) && typeof(T) == typeof(object))))
                {
                    Data = data;
                    return;
                }

                //Transform from anonymous/dynamic object input data to strongly type object if generic type data is provided                
                var properties = new List<PropertyInfo>().AsEnumerable();
                try
                {
                    properties = Data.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
                }
                catch
                {

                }
                if (Data == null && Api.IsGenericType(data.GetType()))
                {
                    Data = (dynamic)Activator.CreateInstance(data.GetType());
                }

                if (data.GetType().FullName == "System.Dynamic.ExpandoObject")
                {
                    foreach (var dict in data)
                    {
                        var val = dict.Value;
                        if ((!Api.IsListType(val.GetType()) && val != null) || (Api.IsListType(val.GetType()) && !Api.IsVoid(val)))
                        {
                            var property = properties.Where(x => x.Name.ToLower() == dict.Key.ToLower()).SingleOrDefault();
                            if (property != null)
                            {
                                if (!Api.IsListType(dict.Value.GetType()))
                                {
                                    if (property.PropertyType.GenericTypeArguments.Count() > 0)
                                    {
                                        property.SetValue(Data, Convert.ChangeType(val, property.PropertyType.GenericTypeArguments[0]));
                                    }
                                    else
                                    {
                                        property.SetValue(Data, Convert.ChangeType(val, property.PropertyType));
                                    }
                                }
                                else
                                {
                                    property.SetValue(Data, (dynamic)val);
                                }
                            }
                        }
                    }
                }
                else
                {
                    var dproperties = new List<PropertyInfo>().AsEnumerable();
                    try
                    {
                        dproperties = data.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
                    }
                    catch
                    {

                    }
                    foreach (var dproperty in dproperties)
                    {
                        var val = dproperty.GetValue(data);
                        if ((!Api.IsListType(dproperty.PropertyType) && val != null) ||
                            (Api.IsListType(dproperty.PropertyType) && !Api.IsVoid(val)))
                        {
                            var property = properties.Where(x => x.Name.ToLower() == dproperty.Name.ToLower()).SingleOrDefault();
                            if (property != null)
                            {
                                if (!Api.IsListType(property.PropertyType))
                                {
                                    if (property.PropertyType.GenericTypeArguments.Count() > 0)
                                    {
                                        property.SetValue(Data, Convert.ChangeType(val, property.PropertyType.GenericTypeArguments[0]));
                                    }
                                    else
                                    {
                                        property.SetValue(Data, Convert.ChangeType(val, property.PropertyType));
                                    }
                                }
                                else
                                {
                                    property.SetValue(Data, (dynamic)val);
                                }
                            }
                        }
                    }
                }
            }
            //Set root table name from data if no custom table name provided
            SetRootTable(data);
        }

        private void QWhere(dynamic data)
        {
            if (data != null)
            {
                if (data.GetType().FullName == "System.Dynamic.ExpandoObject")
                {
                    foreach (var dict in data)
                    {
                        var val = dict.Value;
                        if (!Api.IsListType(val.GetType()) && val != null)
                        {
                            And(dict.Key, "=", val);
                        }
                    }
                }
                else
                {
                    IEnumerable<PropertyInfo> properties = new List<PropertyInfo>().AsEnumerable();
                    try
                    {
                        properties = data.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
                    }
                    catch
                    {

                    }
                    foreach (var property in properties)
                    {
                        if (property.GetValue(data) != null && !Api.IsGenericType(property.PropertyType)
                            && !Api.IsListType(property.PropertyType) && !Api.IsAnonymousType(property.PropertyType))
                        {
                            And(property.Name, "=", property.GetValue(data));
                        }
                    }
                }
            }
        }

        public Sql(T data)
        {
            QSetData(data);
        }

        public Sql(object data)
        {
            QSetData(data);
        }

        public Sql(string table, T data)
        {
            RootTable = table;
            QSetData(data);
        }

        public Sql(string table, object data = null)
        {
            RootTable = table;
            QSetData(data);
        }

        public Sql<T> Configure(DatabaseProvider databaseProvider, string connectionString)
        {
            SqlDatabase = new SqlDatabase(databaseProvider, connectionString) as ISqlDatabase;
            return this;
        }

        public Sql<T> Configure(ISqlConfig sqlConfig)
        {
            SqlDatabase = new SqlDatabase(sqlConfig) as ISqlDatabase;
            return this;
        }

        public Sql<T> Configure(ISqlDatabase sqlDatabase)
        {
            SqlDatabase = sqlDatabase;
            return this;
        }

        public Sql<T> SetData(T data)
        {
            QSetData(data);
            return this;
        }

        public Sql<T> SetData(object data)
        {
            QSetData(data);
            return this;
        }

        private Sql<T> SetIdCol(string table, string column, bool isIdentity = false)
        {
            ClearId(table);
            CriteriaList.Add(new Criteria()
            {
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column,
                IsId = true,
                IsIdentity = isIdentity
            });
            return this;
        }

        public Sql<T> SetManualId(string column)
        {
            return SetIdCol(RootTable, column);
        }

        public Sql<T> SetManualId(string table, string column)
        {
            return SetIdCol(table, column);
        }

        public Sql<T> SetAutoIncrementId(string column)
        {
            return SetIdCol(RootTable, column, true);
        }

        public Sql<T> SetAutoIncrementId(string table, string column)
        {
            return SetIdCol(table, column, true);
        }

        public Sql<T> ClearId(string table = null)
        {
            if (string.IsNullOrEmpty(table))
            {
                CriteriaList.RemoveAll(x => x.IsId);
            }
            else
            {
                CriteriaList.RemoveAll(x => x.IsId && x.Table.ToLower() == table.ToLower());
            }
            return this;
        }

        public Sql<T> ClearPipe()
        {
            CriteriaList.RemoveAll(x => x.Pipe > 0);
            return this;
        }

        public Sql<T> ClearWhere()
        {
            CriteriaList.RemoveAll(x => x.Operator != null || x.Logic > 0 || x.Bracket > 0);
            return this;
        }

        public Sql<T> ClearGroupBy()
        {
            CriteriaList.RemoveAll(x => x.GroupBy);
            return this;
        }

        public Sql<T> ClearOrderBy()
        {
            CriteriaList.RemoveAll(x => x.SortOrder > 0);
            return this;
        }

        public Criteria GetIdCol(IEnumerable<PropertyInfo> parentProperties = null)
        {  
            var properties = new List<PropertyInfo>().AsEnumerable();
            try
            {
                properties = Data.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
            }
            catch
            {

            }
            var idCol = new Criteria() { Table = RootTable, IsId = true, IsIdentity = true };
            if (CriteriaList.Any(x => x.IsId && x.Table.ToLower() == RootTable.ToLower()) || properties.Count() > 0)
            {
                if (parentProperties == null)
                {
                    //Use first class property found (with valid Id type) if custom Id column not provided
                    //Example of valid Id data type can be int, int?, long, long?, string
                    var qIdCol = CriteriaList.Where(x => x.IsId && x.Table.ToLower() == RootTable.ToLower()).FirstOrDefault();
                    if (qIdCol != null)
                    {
                        idCol = qIdCol;
                        idCol.PropertyInfo = properties.Where(x => x.Name.ToLower() == idCol.Column.ToLower()).SingleOrDefault();
                        idCol.Column = idCol.PropertyInfo.Name;
                    }
                    if (idCol.PropertyInfo == null)
                    {
                        var keypi = properties.Where(x => Api.IsIdType(x.PropertyType)).FirstOrDefault();
                        if (keypi != null)
                        {
                            idCol.PropertyInfo = keypi;
                            idCol.Column = idCol.PropertyInfo.Name;
                        }
                    }
                }
                else
                {
                    var linkkeypi = properties.Where(x => parentProperties.Any(y => Api.IsIdType(y.PropertyType) && y.Name.ToLower() == x.Name.ToLower())).FirstOrDefault();
                    if (linkkeypi != null)
                    {
                        idCol.PropertyInfo = linkkeypi;
                        idCol.Column = idCol.PropertyInfo.Name;
                    }
                }
            }
            //Only override Id when non-null value provided
            if (idCol.PropertyInfo != null && Data != null && idCol.Value == null && idCol.PropertyInfo.GetValue(Data) != null)
            {
                idCol.Value = idCol.PropertyInfo.GetValue(Data);
            }
            return idCol;
        }

        private bool? BuildFields(bool isChild, Criteria idCol, IEnumerable<PropertyInfo> properties)
        {
            //Skip all fields & parameters construction when...
            var toAggregate = GetAggregationList().Count > 0;
            var fields = CriteriaList.Where(x => x.Pipe == Pipe.Include || x.Pipe == Pipe.Exclude).ToList();
            var rootFields = fields.Where(x => !x.IsChild).ToList();
            var childFields = fields.Where(x => x.IsChild && x.Table.ToLower() == idCol.Table.ToLower() && (x.Column == "" ||
                properties.Any(y => y.Name.ToLower() == x.Column.ToLower()))).ToList();
            var otherChildFields = fields.Where(x => x.IsChild && x.Table.ToLower() != idCol.Table.ToLower()).ToList();
            bool? skipConstruct = (SqlCommand == SqlCommand.Select && toAggregate) || SqlCommand == SqlCommand.Delete ||
                SqlCommand == SqlCommand.Truncate || CriteriaList.Any(x => x.GroupBy);
            if (!skipConstruct.Value)
            {
                //If skipConstruct will select all child fields except save data action like INSERT/UPDATE/REPLACE, which need to construct fields/parameters
                if (fields.Count == 0)
                {
                    //No action to parent and child, select all parent and child fields
                    skipConstruct = (SqlCommand == SqlCommand.Select);
                }
                else
                {
                    if (isChild)
                    {
                        //Child fields
                        if (childFields.Count == 0)
                        {
                            if (rootFields.Count == 0)
                            {
                                //No action to parent and child
                                if (otherChildFields.Last().Pipe == Pipe.Exclude)
                                {
                                    skipConstruct = (SqlCommand == SqlCommand.Select);
                                }
                                else
                                {
                                    SqlCommand = SqlCommand.Skip;
                                    skipConstruct = null;
                                }
                            }
                            else
                            {
                                if (rootFields.Last().Pipe == Pipe.Exclude)
                                {
                                    //Exclude parent field, select all child fields
                                    //Select all child fields except save data action like INSERT/UPDATE/REPLACE, which need to construct fields/parameters
                                    skipConstruct = (SqlCommand == SqlCommand.Select);
                                }
                                else
                                {
                                    //Include parent field only, exclude all child fields
                                    SqlCommand = SqlCommand.Skip;
                                    skipConstruct = null;
                                }
                            }
                        }
                        else
                        {
                            if (childFields.Last().Pipe == Pipe.Exclude)
                            {
                                if (childFields.Last().Column == "")
                                {
                                    //Exclude child table
                                    SqlCommand = SqlCommand.Skip;
                                    skipConstruct = null;
                                }
                                else
                                {
                                    //Exclude child field
                                    skipConstruct = false;
                                }
                            }
                            else
                            {
                                if (childFields.Last().Column == "")
                                {
                                    //Include child table
                                    skipConstruct = (SqlCommand == SqlCommand.Select);
                                }
                                else
                                {
                                    //Include child field
                                    skipConstruct = false;
                                }
                            }
                        }
                    }
                    else
                    {
                        //Parent fields
                        if (rootFields.Count == 0)
                        {
                            //Some child fields are included/excluded
                            if (fields.Last().Pipe == Pipe.Exclude)
                            {
                                //Include all parent fields
                                skipConstruct = (SqlCommand == SqlCommand.Select);
                            }
                            else
                            {
                                //Include parent id field only
                                if (SqlCommand == SqlCommand.Select)
                                {
                                    //Include parent id field only
                                    skipConstruct = false;
                                }
                                else
                                {
                                    //Exlude all parent fields save data operation
                                    SqlCommand = SqlCommand.Skip;
                                    skipConstruct = null;
                                }
                            }
                        }
                        else
                        {
                            if (rootFields.Last().Column == "")
                            {
                                if (rootFields.Last().Pipe == Pipe.Exclude)
                                {
                                    if (SqlCommand == SqlCommand.Select)
                                    {
                                        //Include parent id field only
                                        skipConstruct = false;
                                    }
                                    else
                                    {
                                        //Exclude all parent fields save data operation
                                        SqlCommand = SqlCommand.Skip;
                                        skipConstruct = null;
                                    }
                                }
                                else
                                {
                                    //Include all parent fields
                                    skipConstruct = (SqlCommand == SqlCommand.Select);
                                }
                            }
                            else
                            {
                                skipConstruct = false;
                            }
                        }
                    }
                }
            }
            if (skipConstruct.HasValue && !skipConstruct.Value)
            {
                var sqlDatabase = GetSqlDatabase();
                var finalSqlCommand = SqlCommand;
                if (SqlCommand == SqlCommand.Replace)
                {
                    if (idCol.Value == null)
                    {
                        finalSqlCommand = SqlCommand.Insert;
                    }
                    else
                    {
                        finalSqlCommand = SqlCommand.Update;
                    }
                }                
                RootFieldList = new List<Criteria>();
                var includeFields = new List<Criteria>().AsEnumerable();
                var excludeFields = new List<Criteria>().AsEnumerable();
                if (isChild)
                {
                    includeFields = childFields.Where(x => x.Pipe == Pipe.Include && x.Column != "");
                    excludeFields = childFields.Where(x => x.Pipe == Pipe.Exclude && x.Column != "");
                }
                else
                {
                    includeFields = rootFields.Where(x => x.Pipe == Pipe.Include && x.Column != "");
                    excludeFields = rootFields.Where(x => x.Pipe == Pipe.Exclude && x.Column != "");
                }
                bool skipGetSQLEscapeName = RootTable.Contains("information_schema");
                for (int p = 0; p < properties.Count(); p++)
                {
                    var pi = properties.ElementAt(p);
                    string field = pi.Name;
                    var val = pi.GetValue(Data);
                    if (sqlDatabase.DatabaseProvider == DatabaseProvider.PostgreSQL && val != null && (pi.PropertyType == typeof(bool) || pi.PropertyType == typeof(bool?)))
                    {
                        val = Convert.ToInt16(val);
                    }

                    //Case when only child data included or all parent fields excluded, need to select at least id field and exit for loop
                    if (!isChild && SqlCommand == SqlCommand.Select && field.ToLower() == idCol.Column.ToLower() && (
                            (rootFields.Count == 0 && fields.Count > 0) ||
                            (rootFields.Count > 0 && fields.Where(x => x.IsChild).ToList().Count == 0 &&
                            rootFields.Last().Pipe == Pipe.Exclude && rootFields.Last().Column == "")
                            )
                        )
                    {
                        RootFieldList.Add(new Criteria() { Table = RootTable, IsChild = isChild, Column = field, PropertyInfo = pi });
                        break;
                    }

                    bool isComplicatedField = pi.PropertyType == typeof(object)|| Api.IsGenericType(pi.PropertyType) || Api.IsListType(pi.PropertyType);
                    bool isIdentiyInsert = finalSqlCommand == SqlCommand.Insert && field.ToLower() == idCol.Column.ToLower() && idCol.IsIdentity;
                    bool isNullOrKeyUpdate = finalSqlCommand == SqlCommand.Update && ((val == null && !includeFields.Any(x => x.UpdateExpression != null)) || field.ToLower() == idCol.Column.ToLower());

                    //Note that Id field cannot be excluded even explicitly defined to be excluded
                    bool isFieldExcluded = !(SqlCommand == SqlCommand.Select && field.ToLower() == idCol.Column.ToLower()) &&
                        excludeFields.Any(x => x.Column.ToLower() == field.ToLower());

                    //Note that if Id field is not included when SELECT it will be included by default, as Id field value is crucial to select child list data
                    bool isFieldIncluded = (SqlCommand == SqlCommand.Select && pi.Name.ToLower() == idCol.Column.ToLower()) || includeFields.Count() == 0 ||
                        includeFields.Any(x => x.Column.ToLower() == field.ToLower());

                    //Skip this field construction when...
                    if (isComplicatedField || isIdentiyInsert || isNullOrKeyUpdate || isFieldExcluded || !isFieldIncluded)
                    {
                        continue;
                    }

                    RootFieldList.Add(new Criteria() { Table = RootTable, IsChild = isChild, Column = field, PropertyInfo = pi });
                }
                //Append columns with pipe `Include` when only anonymous/dynamic data is provided, which is not strongly type defined
                var includeFields2 = includeFields.ToList().Where(x => !properties.Any(y => y.Name.ToLower() == x.Column.ToLower())).AsEnumerable();
                if (!Api.IsGenericType(typeof(T)) && SqlCommand == SqlCommand.Select && includeFields2.Count() > 0)
                {
                    for (int i = 0; i < includeFields2.Count(); i++)
                    {
                        RootFieldList.Add(new Criteria() { Table = RootTable, IsChild = isChild, Column = includeFields2.ElementAt(i).Column });
                    }
                }
            }
            return skipConstruct;
        }

        private string BuildWhere(SqlCommand finalSqlCommand)
        {
            //WHERE statement for all non-INSERT command
            string where = "";           
            if (finalSqlCommand != SqlCommand.Insert)
            {
                var sqlDatabase = GetSqlDatabase();
                var properties = new List<PropertyInfo>().AsEnumerable();
                try
                {
                    properties = Data.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
                }
                catch
                {

                }
                if (properties == null)
                {
                    properties = new List<PropertyInfo>().AsEnumerable();
                }
                where = " WHERE ";
                //Default all WHERE conditions with multiple tables
                var whereColumns = CriteriaList.Where(x => (x.Operator != null || x.Bracket > 0 || x.Logic > 0)).ToList();
                if (RootTable != "")
                {
                    //Filter out only criteria if strongly type data is provided, i.e. a single table
                    whereColumns = whereColumns.Where(x => x.Table.ToLower() == RootTable.ToLower()).ToList();
                }
                for (int w = 0; w < whereColumns.Count; w++)
                {
                    string voidSql = "";
                    var whereColumn = whereColumns[w];
                    if (!string.IsNullOrEmpty(whereColumn.Column))
                    {
                        var wpi = properties.Where(x => x.Name == whereColumn.Column).SingleOrDefault();
                        var val = whereColumn.Value;

                        //Only append table name to WHERE column for JOIN SELECT
                        string colName = (SQL.Contains("JOIN") && whereColumn.Table != "" ? Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, whereColumn.Table) + "." : "") +
                            Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, whereColumn.Column);
                        string colParam = "";

                        if (val == null && wpi != null && wpi.GetValue(Data) != null)
                        {
                            val = wpi.GetValue(Data);
                        }

                        if (whereColumn.Operator == "is" || whereColumn.Operator == "is not")
                        {
                            colParam = " NULL";
                        }
                        else if (whereColumn.Operator == "is void")
                        {
                            voidSql = "(" + colName + " IS NULL";
                            if (wpi != null)
                            {
                                bool setVoidSql = false;
                                if (wpi.PropertyType == typeof(bool) || wpi.PropertyType == typeof(bool?))
                                {
                                    if (sqlDatabase.DatabaseProvider == DatabaseProvider.PostgreSQL)
                                    {
                                        Parameters.Add(whereColumn.Column + w, 0);
                                    }
                                    else
                                    {
                                        Parameters.Add(whereColumn.Column + w, false);
                                    }
                                    setVoidSql = true;
                                }
                                else if (Api.IsNumberType(wpi.PropertyType))
                                {
                                    Parameters.Add(whereColumn.Column + w, 0);
                                    setVoidSql = true;
                                }
                                else if (Api.IsStringType(wpi.PropertyType))
                                {
                                    colName = "TRIM(" + colName + ")";
                                    Parameters.Add(whereColumn.Column + w, "");
                                    setVoidSql = true;
                                }
                                if (setVoidSql)
                                {
                                    voidSql += " OR " + colName + "=@" + whereColumn.Column + w;
                                }
                            }
                            voidSql += ")";
                        }
                        else
                        {
                            colParam = "@" + whereColumn.Column + w;
                            if (val != null)
                            {
                                if (whereColumn.Operator == "like")
                                {
                                    Parameters.Add("@" + whereColumn.Column + w, "%" + val + "%");
                                }
                                else
                                {
                                    if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                                    {
                                        if (val.GetType() == typeof(string) &&
                                            (whereColumn.Operator == "==" || whereColumn.Operator == "!="))
                                        {
                                            colName = "BINARY_CHECKSUM(" + colName + ")";
                                            colParam = "BINARY_CHECKSUM(" + colParam + ")";
                                        }
                                    }
                                    else if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL)
                                    {
                                        if (val.GetType() == typeof(string))
                                        {
                                            if (whereColumn.Operator == "==" || whereColumn.Operator == "!=")
                                            {
                                                //No action needed for information_schema database case sensitive query since default is case sensitive already
                                                if (sqlDatabase.Connection.Database.ToLower() != "information_schema")
                                                {
                                                    colParam = "BINARY " + colParam;
                                                }
                                            }
                                        }
                                    }
                                    else if (sqlDatabase.DatabaseProvider == DatabaseProvider.PostgreSQL)
                                    {
                                        if (val.GetType() == typeof(bool) || val.GetType() == typeof(bool?))
                                        {
                                            val = Convert.ToInt16(val);
                                        }
                                        else if (val.GetType() == typeof(string) &&
                                            (whereColumn.Operator == "==" || whereColumn.Operator == "!="))
                                        {
                                            colName = "LOWER(" + colName + ")";
                                            Parameters.Add(whereColumn.Column + w, val.ToLower());
                                        }
                                    }
                                }
                            }

                            if (!Parameters.ParameterNames.ToList().Exists(x => x == whereColumn.Column + w))
                            {
                                Parameters.Add(whereColumn.Column + w, val);
                            }
                        }
                        string colLogic = "";
                        if (w > 0 && !where.EndsWith("("))
                        {
                            if (whereColumn.Logic == Logic.And)
                            {
                                colLogic = " AND ";
                            }
                            else if (whereColumn.Logic == Logic.Or)
                            {
                                colLogic = " OR ";
                            }
                        }
                        if (voidSql == "")
                        {
                            where += colLogic + colName + " " + whereColumn.Operator + " " + colParam;
                        }
                        else
                        {
                            where += colLogic + voidSql;
                        }
                    }
                    else
                    {
                        //Bracket (Open/Close) or Logic (And/Or)
                        if (whereColumn.Bracket > 0)
                        {
                            if (whereColumn.Bracket == Bracket.Open)
                            {
                                where += "(";
                            }
                            else
                            {
                                where += ")";
                            }
                        }
                        else if (whereColumn.Logic > 0)
                        {
                            if (!where.EndsWith("WHERE "))
                            {
                                if (whereColumn.Logic == Logic.And)
                                {
                                    where += " AND ";
                                }
                                else
                                {
                                    where += " OR ";
                                }
                            }
                        }
                    }
                }
            }
            return where;
        }

        private string BuildGroupBy()
        {
            string groupby = "";
            var groupColumns = new List<Criteria>();
            if (RootTable == "")
            {
                groupColumns = CriteriaList.Where(x => x.GroupBy).ToList();
            }
            else
            {
                groupColumns = CriteriaList.Where(x => x.GroupBy && x.Table.ToLower() == RootTable.ToLower()).ToList();
            }            
            if (SqlCommand == SqlCommand.Select && groupColumns.Count > 0)
            {
                var sqlDatabase = GetSqlDatabase();
                for (int g = 0; g < groupColumns.Count; g++)
                {
                    var groupColumn = groupColumns[g];                    
                    groupby += (SQL.Contains("JOIN") && groupColumn.Table != "" ? (Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, groupColumn.Table) + ".") : "") +
                        Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, groupColumn.Column) + ",";
                }
                groupby = groupby.TrimEnd(',');
            }
            return groupby;
        }

        private string BuildOrderBy()
        {
            string orderby = "";
            var orderColumns = new List<Criteria>();
            if (RootTable == "")
            {
                orderColumns = CriteriaList.Where(x => x.SortOrder > 0 || x.SortCase != "").ToList();
            }
            else
            {
                orderColumns = CriteriaList.Where(x => (x.SortOrder > 0 || x.SortCase != "") && x.Table.ToLower() == RootTable.ToLower()).ToList();
            }
            var aggregations = GetAggregationList();
            if (orderColumns.Count > 0 && SqlCommand == SqlCommand.Select && !(aggregations.Count > 0 && !CriteriaList.Any(x => x.GroupBy)))
            {
                var sqlDatabase = GetSqlDatabase();
                bool firstSortCaseDone = false;
                for (int s = 0; s < orderColumns.Count; s++)
                {
                    var orderColumn = orderColumns[s];
                    if (orderColumn.SortCase == "")
                    {
                        bool skipGetSQLEscapeName = orderColumn.Table.Contains("information_schema");
                        string sqlTable = skipGetSQLEscapeName ? orderColumn.Table : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, orderColumn.Table);
                        string sqlField = skipGetSQLEscapeName ? orderColumn.Column : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, orderColumn.Column);
                        orderby += (SQL.Contains("JOIN") && orderColumn.Table != "" ? (sqlTable + ".") : "") + sqlField +
                            (orderColumn.SortOrder == SortOrder.Desc ? " DESC" : "") + ",";
                    }
                    else
                    {
                        if (firstSortCaseDone)
                        {
                            orderby += " " + orderColumn.SortCase + " ";
                        }
                        else
                        {
                            orderby += "CASE " + orderColumn.SortCase + " ";
                            firstSortCaseDone = true;
                        }
                        if (s == orderColumns.Count - 1 || (s + 1 < orderColumns.Count && orderColumns[s + 1].SortCase == ""))
                        {
                            //END CASE if there is no next sort case
                            orderby += "END,";
                            firstSortCaseDone = false;
                        }
                    }
                }
                orderby = orderby.TrimEnd(',');
            }
            return orderby;
        }

        private string BuildSqlTail(ref string fields, string where, string groupby, string orderby)
        {
            string sqlTail = where;
            var sqlDatabase = GetSqlDatabase();
            int selectLimit = 0;
            int selectPage = 0;
            Criteria limitCol = null;
            if (RootTable == "")
            {
                limitCol = CriteriaList.Where(x => x.Pipe == Pipe.Limit).LastOrDefault();
            }
            else
            {
                limitCol = CriteriaList.Where(x => x.Pipe == Pipe.Limit && x.Table.ToLower() == RootTable.ToLower()).LastOrDefault();
            }
            if (limitCol != null)
            {
                selectLimit = limitCol.Limit;
                selectPage = limitCol.Page;
            }
            if (groupby != "")
            {
                sqlTail += " GROUP BY " + groupby;
                fields = groupby;
            }
            if (orderby != "")
            {
                sqlTail += " ORDER BY " + orderby;
            }
            var aggregations = GetAggregationList();
            if (aggregations.Count > 0)
            {
                if (fields != "")
                {
                    fields += ",";
                }
                for (int g = 0; g < aggregations.Count; g++)
                {
                    var aggregate = aggregations[g];
                    string tableField = "*";
                    if (aggregate.Column != "")
                    {
                        tableField = (SQL.Contains("JOIN") && aggregate.Table != "" ? (Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, aggregate.Table) + ".") : "") +
                            Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, aggregate.Column);
                    }

                    string aggregateKeyword;
                    if (aggregate.Pipe == Pipe.Average)
                    {
                        aggregateKeyword = "AVG";
                    }
                    else if (aggregate.Pipe == Pipe.DistinctCount)
                    {
                        aggregateKeyword = "COUNT(DISTINCT";
                    }
                    else
                    {
                        aggregateKeyword = aggregate.Pipe.ToString().ToUpper();
                    }
                    fields += aggregateKeyword + "(" + tableField + ")" + (aggregate.Pipe == Pipe.DistinctCount ? ")" : "") + " AS ";
                    if (aggregate.Alias == "")
                    {
                        fields += aggregate.Column + aggregate.Pipe.ToString();
                    }
                    else
                    {
                        fields += Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, aggregate.Alias);
                    }
                    fields += ",";
                }
                fields = fields.TrimEnd(',');
            }
            //Limit results by page & rows
            if (selectLimit == 0)
            {
                if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL)
                {
                    sqlTail += ";";
                }
            }
            else
            {
                int offset = 0;
                if (selectPage != 0)
                {
                    offset = (selectPage - 1) * selectLimit;
                }
                if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                {
                    if (offset == 0)
                    {
                        fields = "TOP " + selectLimit.ToString() + " " + fields;
                    }
                    else
                    {
                        sqlTail += " OFFSET " + offset.ToString() + " ROWS FETCH NEXT " + selectLimit.ToString() + " ROWS ONLY";
                    }
                }
                else
                {
                    if (offset == 0)
                    {
                        sqlTail += " LIMIT " + selectLimit.ToString() + ";";
                    }
                    else
                    {
                        if (sqlDatabase.DatabaseProvider == DatabaseProvider.PostgreSQL)
                        {
                            sqlTail += " LIMIT " + selectLimit.ToString() + " OFFSET " + offset.ToString() + ";";
                        }
                        else
                        {
                            sqlTail += " LIMIT " + offset.ToString() + "," + selectLimit.ToString() + ";";
                        }

                    }
                }
            }
            return sqlTail;
        }

        private void BuildSQL(bool isChild = false, bool buildFieldsOnly = false)
        {
            var sqlDatabase = GetSqlDatabase();
            //Check for root table name and Id column
            if (RootTable == "")
            {
                SqlError = "Table name not provided";
                return;
            }
            var idCol = GetIdCol();
            
            //Exit when no WHERE condition can be found to build SELECT query
            if (SqlCommand == SqlCommand.Select && Data != null && Api.IsGenericType(Data.GetType()) &&
                idCol.PropertyInfo == null && !CriteriaList.Any(x => x.Operator != null))
            {
                return;
            }
            //Exit when no data provided for write query
            if ((SqlCommand == SqlCommand.Insert || SqlCommand == SqlCommand.Update ||
                SqlCommand == SqlCommand.Delete || SqlCommand == SqlCommand.Replace) && Data == null)
            {
                SqlError = "Data not provided";
                return;
            }
            var properties = new List<PropertyInfo>().AsEnumerable();
            try
            {
                properties = Data.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public);
            }
            catch
            {

            }
            //Get fields required for read/write query
            bool? skipConstruct = BuildFields(isChild, idCol, properties);
            if (skipConstruct == null || buildFieldsOnly)
            {
                return;
            }
            //StringBuilder for fields/parameters
            var fieldSB = new StringBuilder(); 
            var parameterSB = new StringBuilder();

            //Get SQL escape name for table & Id column
            bool skipGetSQLEscapeName = RootTable.Contains("information_schema");
            string sqlTable = skipGetSQLEscapeName ? RootTable : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, RootTable);
            string sqlIdColumn = skipGetSQLEscapeName ? idCol.Column : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, idCol.Column);

            //Truncate SQL construction (and exit since there is no construction of parameters and WHERE/ORDER BY conditions)
            if (SqlCommand == SqlCommand.Truncate)
            {
                if (sqlDatabase.DatabaseProvider == DatabaseProvider.SQLite3)
                {
                    SQL = "DELETE FROM " + sqlTable + ";";
                }
                else
                {
                    SQL = "TRUNCATE TABLE " + sqlTable + ";";
                }
                return;
            }

            //Set finalSqlCommand based on Id value when command is REPLACE
            var finalSqlCommand = SqlCommand;
            if (idCol.Column != "")
            {
                if (SqlCommand == SqlCommand.Replace)
                {
                    if (idCol.Value == null)
                    {
                        finalSqlCommand = SqlCommand.Insert;
                    }
                    else
                    {
                        finalSqlCommand = SqlCommand.Update;
                    }
                }
                if (idCol.Value != null)
                {
                    Parameters.Add("@" + idCol.Column, idCol.Value);
                }
            }

            //Set fields/parameters
            var updateExpressions = CriteriaList.Where(x => x.Table.ToLower() == RootTable.ToLower() && x.Pipe == Pipe.Include && x.UpdateExpression != null);
            if (RootFieldList.Count > 0)
            {
                for (int f = 0; f < RootFieldList.Count; f++)
                {
                    var pi = RootFieldList[f].PropertyInfo;
                    string field = pi == null ? RootFieldList[f].Column : pi.Name;
                    var val = pi?.GetValue(Data);
                    string sqlField = skipGetSQLEscapeName ? field : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, field);
                    if (SqlCommand == SqlCommand.Select)
                    {
                        fieldSB.Append(sqlField + ",");
                    }
                    else if (finalSqlCommand == SqlCommand.Insert)
                    {
                        fieldSB.Append(sqlField + ",");
                        parameterSB.Append("@" + field + ",");
                        Parameters.Add(field, val);
                    }
                    else if (finalSqlCommand == SqlCommand.Update)
                    {
                        var expressionCriteria = updateExpressions.Where(x => x.Column.ToLower() == field.ToLower()).LastOrDefault();
                        if (expressionCriteria == null)
                        {
                            fieldSB.Append(sqlField + "=@" + field + ",");
                            Parameters.Add(field, val);
                        }
                        else
                        {
                            fieldSB.Append(sqlField + "=");
                            for (int i = 0; i < expressionCriteria.UpdateExpression.Length; i++)
                            {
                                var e = expressionCriteria.UpdateExpression[i];
                                if (Api.IsVoid(e) || expressionCriteria.Column.ToLower() == e.ToLower())
                                {
                                    fieldSB.Append(sqlField);
                                }
                                else
                                {
                                    fieldSB.Append(e);
                                }
                                if (i == expressionCriteria.UpdateExpression.Length - 1)
                                {
                                    fieldSB.Append(",");
                                }
                            }
                        }
                    }
                }
            }

            //Fields in string format
            string fields = "";
            var aggregations = GetAggregationList();
            if (aggregations.Count == 0 && !CriteriaList.Any(x => x.GroupBy))
            {
                fields = "*";
                if (fieldSB.Length > 0)
                {
                    fields = fieldSB.ToString().TrimEnd(',');
                }
            }

            //Parameters in string format
            string parameters = "";
            if (parameterSB.Length > 0)
            {
                parameters = parameterSB.ToString().TrimEnd(',');
            }

            //Construct WHERE statement
            string where = "";
            if (CriteriaList.Any(x => x.Table.ToLower() == RootTable.ToLower() && x.Operator != null))
            {
                where = BuildWhere(finalSqlCommand);
            }
            else
            {                
                //No WHERE condition set, use Id value to construct WHERE
                if (idCol != null && idCol.Value != null)
                {
                    where = " WHERE " + sqlIdColumn + "=@" + idCol.Column;
                }
                else
                {
                    //UPDATE to whole table without any condition set is allowed only via mannual commit
                    if ((finalSqlCommand == SqlCommand.Update || SqlCommand == SqlCommand.Delete) && (SqlFactory.AutoCommit || (AutoCommit.HasValue && AutoCommit.Value)))
                    {
                        SqlError = "Update/Delete table without condition set is allowed only via manual commit";
                        return;
                    }
                }
            }

            //Construct GROUP BY columns if command is SELECT
            string groupby = BuildGroupBy();

            //Construct ORDER BY columns, skip if command is to INSERT/UPDATE/DELETE or aggregate SELECT without GROUP BY columns
            string orderby = BuildOrderBy();

            //Construct final SQL
            if (SqlCommand == SqlCommand.Select)
            {
                string sqlTail = BuildSqlTail(ref fields, where, groupby, orderby);
                SQL = "SELECT " + fields + " FROM " + sqlTable + sqlTail;
            }
            else if (finalSqlCommand == SqlCommand.Insert)
            {
                //Here we may need to reuse an id value after insert
                string idInsertedQuery = "";
                if (idCol.Column != "")
                {  
                    if (idCol.IsIdentity)
                    {
                        //Identity Id column where data type is int, int?, long, or long?                        
                        if (sqlDatabase.DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
                        {
                            idInsertedQuery = "SELECT SCOPE_IDENTITY();";
                        }
                        else if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL)
                        {
                            idInsertedQuery = "SELECT LAST_INSERT_ID();";
                        }
                        else if (sqlDatabase.DatabaseProvider == DatabaseProvider.PostgreSQL)
                        {
                            idInsertedQuery = "SELECT currval(pg_get_serial_sequence('" + idCol.Table.ToLower() + "','" + idCol.Column.ToLower() + "'));";
                        }
                        else if (sqlDatabase.DatabaseProvider == DatabaseProvider.SQLite3)
                        {
                            idInsertedQuery = "SELECT last_insert_rowid();";
                        }
                    }
                    else
                    {
                        //Non-Identity column where data type is String or Id is provided
                        idInsertedQuery = " SELECT @" + idCol.Column + ";";
                    }
                }
                SQL = "INSERT INTO " + sqlTable + "(" + fields + ") VALUES(" + parameters + ");" + idInsertedQuery;
            }
            else if (finalSqlCommand == SqlCommand.Update)
            {
                SQL = "UPDATE " + sqlTable + " SET " + fields + " " + where;
            }
            else if (SqlCommand == SqlCommand.Delete)
            {
                SQL = "DELETE FROM " + sqlTable + " " + where;
            }
        }

        public Sql<T> IncludeColumn(string table, string column, string[] updateExpression = null)
        {            
            CriteriaList.Add(new Criteria()
            {
                Pipe = Pipe.Include,
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column,
                UpdateExpression = updateExpression
            });
            return this;
        }

        public Sql<T> IncludeColumn(string column, string[] updateExpression = null)
        {            
            return IncludeColumn(RootTable, column, updateExpression);
        }

        public Sql<T> IncludeColumns(string[] columns, string[] updateExpression = null)
        {
            foreach (var column in columns)
            {
                IncludeColumn(column, updateExpression);
            }
            return this;
        }

        public Sql<T> IncludeColumns(string table, string[] columns, string[] updateExpression = null)
        {
            foreach (var column in columns)
            {
                IncludeColumn(table, column, updateExpression);
            }
            return this;
        }

        public Sql<T> IncludeRootColumns()
        {
            IncludeColumn("");
            return this;
        }

        public Sql<T> IncludeTable(string table)
        {
            IncludeColumn(table, "");
            return this;
        }

        public Sql<T> IncludeTables(string[] tables)
        {
            foreach(var table in tables)
            {
                IncludeTable(table);
            }
            return this;
        }

        public Sql<T> ExcludeColumn(string table, string column)
        {            
            CriteriaList.Add(new Criteria()
            {
                Pipe = Pipe.Exclude,
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column
            });
            return this;
        }

        public Sql<T> ExcludeColumn(string column)
        {            
            return ExcludeColumn(RootTable, column);
        }

        public Sql<T> ExcludeColumns(string[] columns)
        {
            foreach (var column in columns)
            {
                ExcludeColumn(column);
            }
            return this;
        }

        public Sql<T> ExcludeColumns(string table, string[] columns)
        {
            foreach (var column in columns)
            {
                ExcludeColumn(table, column);
            }
            return this;
        }

        public Sql<T> ExcludeRootColumns()
        {
            ExcludeColumn("");
            return this;
        }

        public Sql<T> ExcludeTable(string table)
        {
            ExcludeColumn(table, "");
            return this;
        }

        public Sql<T> ExcludeTables(string[] tables)
        {
            foreach(var table in tables)
            {
                ExcludeTable(table);
            }
            return this;
        }

        public Sql<T> Limit(string table, int selectLimit, int selectPage = 0)
        {
            CriteriaList.Add(new Criteria()
            {
                Pipe = Pipe.Limit,
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Limit = selectLimit,
                Page = selectPage
            });
            return this;
        }

        public Sql<T> Limit(int selectLimit, int selectPage = 0)
        {            
            Limit(RootTable, selectLimit, selectPage);
            return this;
        }

        public Sql<T> Where(T data)
        {
            QWhere(data);
            return this;
        }

        public Sql<T> Where(object data)
        {
            QWhere(data);
            return this;
        }

        private Sql<T> Where(Bracket bracket, Logic logic, string table, string column, string comparator, dynamic value)
        {
            string[] sqlSigns = new[] { null, "=", "<>", "==", "!=", "<", ">", "<=", ">=", "is", "is not", "like", "is void" };
            string _operator = sqlSigns.Where(x => x == comparator.Trim().ToLower()).SingleOrDefault();
            CriteriaList.Add(new Criteria()
            {
                Bracket = bracket,
                Logic = logic,
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column,
                Operator = _operator,
                Value = value
            });
            return this;
        }

        public Sql<T> And(string table, string column, string comparator, dynamic value)
        {
            if (!Api.IsVoid(value) && Api.IsArrayType(value.GetType()))
            {
                foreach (var v in value)
                {
                    Where(Bracket.None, Logic.And, table, column, comparator, v);
                }
            }
            else
            {
                Where(Bracket.None, Logic.And, table, column, comparator, value);
            }
            return this;
        }

        public Sql<T> And(string table, string[] columns, string comparator, dynamic value)
        {
            foreach (var column in columns)
            {
                And(table, column, comparator, value);
            }
            return this;
        }

        public Sql<T> And(string column, string comparator, dynamic value)
        {
            And(RootTable, column, comparator, value);
            return this;
        }

        public Sql<T> And(string[] columns, string comparator, dynamic value)
        {
            foreach (var column in columns)
            {
                And(column, comparator, value);
            }
            return this;
        }

        public Sql<T> Or(string table, string column, string comparator, dynamic value)
        {
            if (!Api.IsVoid(value) && Api.IsArrayType(value.GetType()))
            {
                foreach (var v in value)
                {
                    Where(Bracket.None, Logic.Or, table, column, comparator, v);
                }
            }
            else
            {
                Where(Bracket.None, Logic.Or, table, column, comparator, value);
            }
            return this;
        }

        public Sql<T> Or(string table, string[] columns, string comparator, dynamic value)
        {
            foreach (var column in columns)
            {
                Or(table, column, comparator, value);
            }
            return this;
        }

        public Sql<T> Or(string column, string comparator, object value)
        {
            Or(RootTable, column, comparator, value);
            return this;
        }

        public Sql<T> Or(string[] columns, string comparator, object value)
        {
            foreach (var column in columns)
            {
                Or(column, comparator, value);
            }
            return this;
        }

        public Sql<T> OpenBracket()
        {
            CriteriaList.Add(new Criteria()
            {
                Table = RootTable,
                Bracket = Bracket.Open
            });
            return this;
        }

        public Sql<T> CloseBracket()
        {
            CriteriaList.Add(new Criteria()
            {
                Table = RootTable,
                Bracket = Bracket.Close
            });
            return this;
        }

        public Sql<T> And()
        {
            CriteriaList.Add(new Criteria()
            {
                Table = RootTable,
                Logic = Logic.And
            });
            return this;
        }

        public Sql<T> Or()
        {
            CriteriaList.Add(new Criteria()
            {
                Table = RootTable,
                Logic = Logic.Or
            });
            return this;
        }

        public Sql<T> GroupBy(string table, string column)
        {            
            CriteriaList.Add(new Criteria()
            {
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column,
                GroupBy = true
            });
            return this;
        }

        public Sql<T> GroupBy(string column)
        {            
            return GroupBy(RootTable, column);
        }

        public Sql<T> GroupBy(string[] columns)
        {
            foreach(var column in columns)
            {
                GroupBy(column);
            }
            return this;
        }

        public Sql<T> GroupBy(string table, string[] columns)
        {
            foreach (var column in columns)
            {
                GroupBy(table, column);
            }
            return this;
        }

        private Sql<T> OrderBy(string table, string column, SortOrder sortOrder = SortOrder.Asc)
        {            
            CriteriaList.Add(new Criteria()
            {
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column,
                SortOrder = sortOrder
            });
            return this;
        }

        public Sql<T> OrderBy(string table, string column)
        {
            return OrderBy(table, column, SortOrder.Asc);
        }

        public Sql<T> OrderBy(string table, string[] columns)
        {
            foreach (var column in columns)
            {
                OrderBy(table, column);
            }
            return this;
        }

        public Sql<T> OrderBy(string column)
        {
            return OrderBy(RootTable, column);
        }

        public Sql<T> OrderBy(string[] columns)
        {
            foreach (var column in columns)
            {
                OrderBy(column);
            }
            return this;
        }

        public Sql<T> OrderByDesc(string table, string column)
        {
            return OrderBy(table, column, SortOrder.Desc);
        }

        public Sql<T> OrderByDesc(string table, string[] columns)
        {
            foreach (var column in columns)
            {
                OrderByDesc(table, column);
            }
            return this;
        }

        public Sql<T> OrderByDesc(string column)
        {
            return OrderByDesc(RootTable, column);
        }

        public Sql<T> OrderByDesc(string[] columns)
        {
            foreach (var column in columns)
            {
                OrderByDesc(column);
            }
            return this;
        }

        public Sql<T> OrderByCase(string table, string sort_case)
        {
            CriteriaList.Add(new Criteria()
            {
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                SortCase = sort_case
            });
            return this;
        }

        public Sql<T> OrderByCase(string sort_case)
        {
            return OrderByCase(RootTable, sort_case);
        }

        public Sql<T> OrderByCases(string table, string[] sort_cases)
        {
            foreach (var sort_case in sort_cases)
            {
                OrderByCase(table, sort_case);
            }
            return this;
        }

        public Sql<T> OrderByCases(string[] sort_cases)
        {
            foreach (var sort_case in sort_cases)
            {
                OrderByCase(sort_case);
            }
            return this;
        }

        internal void RunSelectSQL(IDbConnection connection, bool isChild = false, bool buildSQL = true)
        {
            if (SQL.Contains("JOIN"))
            {
                string fields = "";
                string tablejoins = "";
                string where = BuildWhere(SqlCommand.Select);
                string groupby = BuildGroupBy();
                string orderby = BuildOrderBy();
                if (groupby == "")
                {
                    fields = SQL.Split(" FROM")[0].Split("SELECT ")[1];
                }
                else
                {
                    fields = groupby;                    
                }
                tablejoins = SQL.Split("FROM")[1];
                string sqlTail = BuildSqlTail(ref fields, where, groupby, orderby);
                SQL = "SELECT " + fields + " FROM " + tablejoins + sqlTail;
            }
            else
            {
                if (buildSQL)
                {
                    BuildSQL(isChild);
                }
            }
            buildSQL = true;
            if (SQL != "")
            {
                try
                {                    
                    var aggregations = GetAggregationList();
                    if (aggregations.Count > 0)
                    {
                        if (aggregations.Count == 1 && (aggregations[0].Pipe == Pipe.Count || aggregations[0].Pipe == Pipe.DistinctCount) && !CriteriaList.Any(x => x.GroupBy))
                        {
                            Count = connection.QuerySingleOrDefault<int>(SQL, Parameters);
                        }
                        else
                        {
                            DynamicList = connection.Query(SQL, Parameters).ToList();
                        }
                        return;
                    }
                    else
                    {
                        if (Api.IsGenericType(typeof(T)))
                        {                            
                            List = connection.Query<T>(SQL, Parameters).ToList();
                        }
                        else if (Data != null && Api.IsGenericType(Data.GetType()))
                        {
                            List = (dynamic)connection.Query(Data.GetType(), SQL, Parameters).ToList();
                        }
                        else
                        {
                            List = connection.Query<T>(SQL, Parameters).ToList();
                        }
                        Count = List.Count;
                    }
                }
                catch (Exception ex)
                {
                    SqlError = ex.ToString();
                }
            }
            Type genericType = null;
            if (Api.IsGenericType(typeof(T)))
            {
                genericType = typeof(T);
            }
            else if (Data != null && Api.IsGenericType(Data.GetType()))
            {
                genericType = Data.GetType();
            }
            if (genericType == null)
            {
                return;
            }
            if (SQL == "")
            {
                List.Add((dynamic)Activator.CreateInstance(genericType));
            }
            var properties = new List<PropertyInfo>().AsEnumerable();
            try
            {
                properties = List[0].GetType().GetProperties().AsEnumerable();
            }
            catch
            {

            }
            if (SqlError == "" && (SQL == "" || !Api.IsVoid(List)) && properties.Any(x => Api.IsGenericType(x.PropertyType) || Api.IsGenericListType(x.PropertyType)))
            {
                //Only process generic (list) properties
                var gProperties = properties.Where(x => Api.IsGenericType(x.PropertyType) || Api.IsGenericListType(x.PropertyType));
                for (int p = 0; p < gProperties.Count(); p++)
                {
                    PropertyInfo pi = gProperties.ElementAt(p);
                    bool isList = false;
                    dynamic propInstance = null;
                    if (Api.IsListType(pi.PropertyType))
                    {
                        isList = true;
                        propInstance = Api.CreateGenericListInstance(pi.PropertyType);
                        if (propInstance != null)
                        {
                            propInstance = propInstance[0];
                        }
                    }
                    else
                    {
                        propInstance = Activator.CreateInstance(pi.PropertyType);
                    }
                    if (propInstance != null)
                    {
                        //Even though we have a list of same class data, but we only need 1 Sql<T> details to build & run SQL for child list data
                        var propSqlType = typeof(Sql<>).MakeGenericType(propInstance.GetType());
                        dynamic propQ = Activator.CreateInstance(propSqlType, propInstance);
                        propQ.SqlDatabase = SqlDatabase;
                        propQ.SqlCommand = SqlCommand;
                        propQ.CriteriaList = new List<Criteria>(CriteriaList);
                        propQ.BuildSQL(true);
                        if (propQ.SqlCommand != SqlCommand.Skip)
                        {
                            if (SQL == "")
                            {
                                propQ.RunSelectSQL(connection, true);
                                propQ.ResolveSQL();
                                pi.SetValue(List[0], propQ.List);
                            }
                            else
                            {
                                var propIdCol = propQ.GetIdCol(properties);
                                propIdCol.IsChild = true;
                                if (propIdCol != null)
                                {
                                    var linkkeypi = properties.Where(x => x.Name.ToLower() == propIdCol.Column.ToLower()).SingleOrDefault();
                                    //For each parent data, set child Id from parent Id
                                    for (int i = 0; i < List.Count; i++)
                                    {
                                        var linkId = linkkeypi.GetValue(List[i]);
                                        if (linkId != null)
                                        {
                                            if (i == 0)
                                            {
                                                propIdCol.PropertyInfo.SetValue(propQ.Data, linkId);
                                                propIdCol.Value = linkId;
                                                propQ.CriteriaList.Add(propIdCol);
                                                propQ.RunSelectSQL(connection, true);
                                            }
                                            else
                                            {
                                                propQ.Parameters.Add(propIdCol.Column, linkId);
                                                propQ.RunSelectSQL(connection, true, false);
                                            }
                                            if (isList)
                                            {
                                                pi.SetValue(List[i], propQ.List);
                                            }
                                            else
                                            {
                                                if (!Api.IsVoid(propQ.List)) {
                                                    pi.SetValue(List[i], propQ.List[0]);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        private void RunSaveSQLFunc(IDbConnection connection)
        {
            if (SQL != "")
            {
                if (SQL.StartsWith("INSERT"))
                {
                    Count += 1;
                    string newIdVal = null;
                    try
                    {
                        newIdVal = connection.Query<string>(SQL, Parameters).SingleOrDefault();
                    }
                    catch (Exception ex)
                    {
                        SqlError = ex.ToString();
                    }
                    if (!string.IsNullOrEmpty(newIdVal))
                    {
                        var idCol = GetIdCol();
                        idCol.Value = Convert.ChangeType(newIdVal, idCol.PropertyInfo.PropertyType.GenericTypeArguments[0]);
                        if (Api.IsGenericType(Data.GetType()))
                        {
                            idCol.PropertyInfo.SetValue(Data, idCol.Value);
                        }
                        Result += 1;
                    }
                }
                else if (SqlCommand == SqlCommand.Update || SqlCommand == SqlCommand.Delete || SqlCommand == SqlCommand.Replace)
                {
                    try
                    {
                        int result = connection.Execute(SQL, Parameters);
                        if (result > 0)
                        {
                            Count += result;
                        }
                        Result += result;
                    }
                    catch (Exception ex)
                    {
                        SqlError = ex.ToString();
                    }
                }                
                else if (SqlCommand == SqlCommand.Truncate)
                {
                    try
                    {
                        connection.Execute(SQL, Parameters);
                    }
                    catch (Exception ex)
                    {
                        SqlError = ex.ToString();
                    }
                }
            }
        }

        internal void RunSaveSQL(IDbConnection connection, bool isChild = false)
        {
            var parentCmd = SqlCommand;
            BuildSQL(isChild);
            if (SqlError != "")
            {
                return;
            }
            if (SQL != "" && SqlCommand != SqlCommand.Skip)
            {                
                RunSaveSQLFunc(connection);
            }
            var properties = Data.GetType().GetProperties().AsEnumerable();
            if (Count == Result && SqlError == "" && properties.Any(x => Api.IsGenericType(x.PropertyType) || Api.IsGenericListType(x.PropertyType)))
            {
                var gProperties = properties.Where(x => Api.IsGenericType(x.PropertyType) || Api.IsGenericListType(x.PropertyType));
                for (int p = 0; p < gProperties.Count(); p++)
                {
                    var pi = gProperties.ElementAt(p);
                    bool isList = false;
                    dynamic propData = null;
                    if (Api.IsListType(pi.PropertyType))
                    {
                        isList = true;
                        //Filter out list data which is null
                        propData = new List<dynamic>();
                        dynamic list = pi.GetValue(Data);
                        for (int i = 0; i < list.Count; i++)
                        {
                            if (list[i] != null)
                            {
                                propData.Add(list[i]);
                            }
                        }
                    }
                    else
                    {
                        propData = pi.GetValue(Data);
                    }
                    //Ensure it is not null or an empty list
                    if (!Api.IsVoid(propData))
                    {
                        //Even though we have a list of same class data, but we only need 1 Sql<T> details to build & run SQL for child list data
                        dynamic propInstance = null;
                        if (isList)
                        {
                            propInstance = Api.CreateGenericListInstance(pi.PropertyType);
                            if (propInstance != null)
                            {
                                propInstance = propInstance[0];
                            }
                        }
                        else
                        {
                            propInstance = Activator.CreateInstance(pi.PropertyType);
                        }
                        if (propInstance != null)
                        {
                            var propSqlType = typeof(Sql<>).MakeGenericType(propInstance.GetType());
                            var propQ = Activator.CreateInstance(propSqlType, propInstance);
                            propQ.SqlDatabase = SqlDatabase;
                            propQ.SqlCommand = parentCmd;
                            propQ.CriteriaList = new List<Criteria>(CriteriaList);
                            propQ.BuildSQL(true);
                            if (propQ.SqlCommand != SqlCommand.Skip)
                            {
                                var propIdCol = propQ.GetIdCol();
                                var propProperties = ((PropertyInfo[])propInstance.GetType().GetProperties()).AsEnumerable();
                                //For each child, fill matched Id value from parent
                                //Note that child Id field is not same as the matched Id field between parent and child
                                //Also assign all non-null parent field values to matched child fields with null value
                                //Only proceed to save child data when a link id value is not null
                                var linkIdCol = propQ.GetIdCol(properties);
                                var parentLinkIdProperty = properties.Where(x => x.Name.ToLower() == linkIdCol.Column.ToLower()).SingleOrDefault();
                                var childLinkIdProperty = propProperties.Where(x => x.Name.ToLower() == linkIdCol.Column.ToLower()).SingleOrDefault();
                                var matchProperties = properties.Where(x => propProperties.Any(y => y.Name.ToLower() != linkIdCol.Column.ToLower() && y.Name.ToLower() == x.Name.ToLower())).ToList();
                                int count = 1;
                                if (isList)
                                {
                                    count = propData.Count;
                                }
                                for (int i = 0; i < count; i++)
                                {
                                    dynamic parentLinkIdValue = null;
                                    if (parentLinkIdProperty != null)
                                    {
                                        parentLinkIdValue = parentLinkIdProperty.GetValue(Data);
                                    }

                                    dynamic childLinkIdValue = null;
                                    if (isList)
                                    {
                                        childLinkIdValue = childLinkIdProperty.GetValue(propData[i]);
                                    }
                                    else
                                    {
                                        childLinkIdValue = childLinkIdProperty.GetValue(propData);
                                    }
                                    if (parentLinkIdValue != null && childLinkIdValue == null)
                                    {
                                        if (isList)
                                        {
                                            childLinkIdProperty.SetValue(propData[i], parentLinkIdValue);
                                        }
                                        else
                                        {
                                            childLinkIdProperty.SetValue(propData, parentLinkIdValue);
                                        }
                                    }

                                    //Check again after assignment from parent
                                    if (isList)
                                    {
                                        childLinkIdValue = childLinkIdProperty.GetValue(propData[i]);
                                    }
                                    else
                                    {
                                        childLinkIdValue = childLinkIdProperty.GetValue(propData);
                                    }

                                    //Assign all child null fields with matched parent non-null field values
                                    for (int m = 0; m < matchProperties.Count; m++)
                                    {
                                        var childProp = propProperties.Where(x => x.Name.ToLower() == matchProperties[m].Name.ToLower()).SingleOrDefault();

                                        if (matchProperties[m].GetValue(Data) != null)
                                        {
                                            if (isList)
                                            {
                                                if (childProp.GetValue(propData[i]) == null)
                                                {
                                                    childProp.SetValue(propData[i], matchProperties[m].GetValue(Data));
                                                }
                                            }
                                            else
                                            {
                                                if (childProp.GetValue(propData) == null)
                                                {
                                                    childProp.SetValue(propData, matchProperties[m].GetValue(Data));
                                                }
                                            }
                                        }
                                    }

                                    if (propIdCol != null && childLinkIdValue != null)
                                    {
                                        if (isList)
                                        {
                                            propQ.Data = propData[i];
                                        }
                                        else
                                        {
                                            propQ.Data = propData;
                                        }
                                        propQ.Count = 0;
                                        propQ.Result = 0;
                                        propQ.SqlError = "";
                                        propQ.RunSaveSQL(connection, true);
                                        Count += propQ.Count;
                                        Result += propQ.Result;
                                        if (propQ.Count == propQ.Result && propQ.SqlError == "")
                                        {
                                            if (isList)
                                            {
                                                propData[i] = propQ.Data;
                                            }
                                            else
                                            {
                                                propData = propQ.Data;
                                            }
                                        }
                                        else
                                        {
                                            SqlCommand = parentCmd;
                                            SQL = propQ.SQL;
                                            SqlError = propQ.SqlError;
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                SqlCommand = parentCmd;
            }
        }

        internal ISqlDatabase GetSqlDatabase()
        {
            if (SqlDatabase == null)
            {
                return SqlFactory.SqlDatabase;
            }
            else
            {
                return SqlDatabase;
            }
        }

        private Sql<T> QSelect(bool isChild = false)
        {
            using (var connection = GetSqlDatabase().Open())
            {
                SqlCommand = SqlCommand.Select;
                RunSelectSQL(connection, isChild);
                ResolveSQL();
            }
            return this;
        }

        public Sql<T> Select(object data = null)
        {
            Where(data);
            return QSelect();
        }

        public List<T> GetList(object data = null)
        {
            Where(data);
            return QSelect().List;
        }

        public T GetSingle(object data = null)
        {
            GetList(data);
            if (List.Count == 1)
            {
                return List[0];
            }
            else
            {
                return default;
            }
        }

        public dynamic GetDynamicList(object data = null)
        {
            Where(data);
            return QSelect().DynamicList;
        }

        public dynamic GetDynamicSingle(object data = null)
        {
            GetDynamicList(data);
            if (DynamicList.Count == 1)
            {
                return DynamicList[0];
            }
            else
            {
                return default;
            }
        }

        public dynamic GetCount(object data = null)
        {
            var q = new Sql<T>().Where(data);
            q.CriteriaList.Add(new Criteria { Pipe = Pipe.Count });
            q.CriteriaList.AddRange(CriteriaList.Where(x => x.Logic > 0 || x.Bracket > 0));
            return q.QSelect().Count;
        }

        public dynamic GetDistinctCount(string table, string column)
        {
            var q = new Sql<T>();
            q.AddTableDistinctCount(table, column);
            q.CriteriaList.AddRange(CriteriaList.Where(x => x.Logic > 0 || x.Bracket > 0));
            return q.QSelect().Count;
        }

        public dynamic GetDistinctCount(string column)
        {
            return GetDistinctCount(RootTable, column);
        }

        private List<Criteria> GetAggregationList()
        {
            return CriteriaList.Where(x => x.Pipe == Pipe.Count || x.Pipe == Pipe.DistinctCount || x.Pipe == Pipe.Min || x.Pipe == Pipe.Max ||
                x.Pipe == Pipe.Sum || x.Pipe == Pipe.Average).ToList();
        }

        private Sql<T> AddAggregate(Pipe pipe, string table = "", string column = "", string alias = "")
        {
            CriteriaList.Add(new Criteria
            {
                Pipe = pipe,
                Table = table,
                IsChild = table.ToLower() != RootTable.ToLower(),
                Column = column,
                Alias = alias
            });
            return this;
        }

        public Sql<T> AddTableCount(string table = "", string column = "", string alias = "")
        {            
            return AddAggregate(Pipe.Count, table, column, alias);
        }

        public Sql<T> AddCount(string column = "", string alias = "")
        {
            return AddAggregate(Pipe.Count, RootTable, column, alias);
        }
        public Sql<T> AddTableDistinctCount(string table = "", string column = "", string alias = "")
        {
            return AddAggregate(Pipe.DistinctCount, table, column, alias);
        }

        public Sql<T> AddDistinctCount(string column = "", string alias = "")
        {
            return AddAggregate(Pipe.DistinctCount, RootTable, column, alias);
        }

        public Sql<T> AddTableMin(string table, string column, string alias = "")
        {
            return AddAggregate(Pipe.Min, table, column, alias);
        }

        public Sql<T> AddMin(string column, string alias = "")
        {
            return AddAggregate(Pipe.Min, RootTable, column, alias);
        }

        public Sql<T> AddTableMax(string table, string column, string alias = "")
        {
            return AddAggregate(Pipe.Max, table, column, alias);
        }

        public Sql<T> AddMax(string column, string alias = "")
        {
            return AddAggregate(Pipe.Max, RootTable, column, alias);
        }

        public Sql<T> AddTableSum(string table, string column, string alias = "")
        {
            return AddAggregate(Pipe.Sum, table, column, alias);
        }

        public Sql<T> AddSum(string column, string alias = "")
        {
            return AddAggregate(Pipe.Sum, RootTable, column, alias);
        }

        public Sql<T> AddTableAverage(string table, string column, string alias = "")
        {
            return AddAggregate(Pipe.Average, table, column, alias);
        }

        public Sql<T> AddAverage(string column, string alias = "")
        {
            return AddAggregate(Pipe.Average, RootTable, column, alias);
        }

        private Sql<T> QSave(dynamic data = null)
        {
            //Save single set of data to a table OR
            //Delete data based on Id/WHERE conditions OR
            //Truncate table, need to commit manually
            var disableTransactionScope = DisableTransactionScope == null ? SqlFactory.DisableTransactionScope : DisableTransactionScope.Value;
            var autoCommit = AutoCommit == null ? SqlFactory.AutoCommit : AutoCommit.Value;
            var isolationLevel = IsolationLevel == null ? SqlFactory.IsolationLevel : IsolationLevel.Value;
            using (TransactionScope transactionScope = (disableTransactionScope || !autoCommit || SqlCommand == SqlCommand.Truncate) ? null :
                new TransactionScope(TransactionScopeOption.Required, new TransactionOptions() { IsolationLevel = isolationLevel }))
            {
                using (var connection = GetSqlDatabase().Open())
                {
                    if (data != null)
                    {
                        QSetData(data);
                    }
                    if ((!autoCommit || SqlCommand == SqlCommand.Truncate) && Transaction.Current == null)
                    {
                        SqlError = "Transaction scope must be provided for manual commit";
                    }
                    else
                    {
                        Count = 0;
                        Result = 0;
                        SqlError = "";
                        Errors = new List<Error>();
                        RunSaveSQL(connection);
                    }                    
                    ResolveSQL();
                    if (transactionScope != null && SqlError == "" &&
                        Count > 0 && Result > 0 && Count == Result && Errors.Count == 0)
                    {
                        transactionScope.Complete();
                    }
                }
            }
            return this;
        }

        public Sql<T> Save(T data)
        {
            if (SqlCommand != SqlCommand.Insert && SqlCommand != SqlCommand.Update && SqlCommand != SqlCommand.Delete && SqlCommand != SqlCommand.Truncate)
            {
                SqlCommand = SqlCommand.Replace;
            }
            return QSave(data);
        }

        public Sql<T> Insert(T data)
        {
            SqlCommand = SqlCommand.Insert;
            return QSave(data);
        }

        public Sql<T> Update(T data)
        {
            SqlCommand = SqlCommand.Update;
            return QSave(data);
        }

        public Sql<T> Delete(T data)
        {
            SqlCommand = SqlCommand.Delete;
            return QSave(data);
        }

        public Sql<T> Save(object data = null)
        {
            if (SqlCommand != SqlCommand.Insert && SqlCommand != SqlCommand.Update && SqlCommand != SqlCommand.Delete && SqlCommand != SqlCommand.Truncate)
            {
                SqlCommand = SqlCommand.Replace;
            }
            return QSave(data);
        }

        public Sql<T> Insert(object data = null)
        {
            SqlCommand = SqlCommand.Insert;
            return QSave(data);
        }

        public Sql<T> Update(object data = null)
        {
            SqlCommand = SqlCommand.Update;
            return QSave(data);
        }

        public Sql<T> Delete(object data = null)
        {
            SqlCommand = SqlCommand.Delete;
            return QSave(data);
        }

        private Sql<T> Truncate()
        {
            SqlCommand = SqlCommand.Truncate;
            return QSave();
        }

        private Sql<T> ToSave(SqlCommand sqlCmd)
        {
            SqlCommand = sqlCmd;
            return this;
        }

        private Sql<T> ToSave(SqlCommand sqlCmd, T data)
        {
            SqlCommand = sqlCmd;
            QSetData(data);
            return this;
        }

        private Sql<T> ToSave(SqlCommand sqlCmd, object data)
        {
            SqlCommand = sqlCmd;
            QSetData(data);
            return this;
        }

        public Sql<T> ToSave(T data)
        {
            if (SqlCommand != SqlCommand.Insert && SqlCommand != SqlCommand.Update && SqlCommand != SqlCommand.Delete && SqlCommand != SqlCommand.Truncate)
            {
                SqlCommand = SqlCommand.Replace;
            }
            return ToSave(SqlCommand, data);
        }

        public Sql<T> ToInsert(T data)
        {
            return ToSave(SqlCommand.Insert, data);
        }

        public Sql<T> ToUpdate(T data)
        {
            return ToSave(SqlCommand.Update, data);
        }

        public Sql<T> ToDelete(T data)
        {
            return ToSave(SqlCommand.Delete, data);
        }

        public Sql<T> ToSave(object data = null)
        {
            if (SqlCommand != SqlCommand.Insert && SqlCommand != SqlCommand.Update && SqlCommand != SqlCommand.Delete && SqlCommand != SqlCommand.Truncate)
            {
                SqlCommand = SqlCommand.Replace;
            }
            return ToSave(SqlCommand, data);
        }

        public Sql<T> ToInsert(object data = null)
        {
            return ToSave(SqlCommand.Insert, data);
        }

        public Sql<T> ToUpdate(object data = null)
        {
            return ToSave(SqlCommand.Update, data);
        }

        public Sql<T> ToDelete(object data = null)
        {
            return ToSave(SqlCommand.Delete, data);
        }

        public Sql<T> ToTruncate()
        {
            return ToSave(SqlCommand.Truncate);
        }

        private Sql<T> QSaveList(List<T> list = null)
        {
            var disableTransactionScope = DisableTransactionScope == null ? SqlFactory.DisableTransactionScope : DisableTransactionScope.Value;
            var autoCommit = AutoCommit == null ? SqlFactory.AutoCommit : AutoCommit.Value;
            var isolationLevel = IsolationLevel == null ? SqlFactory.IsolationLevel : IsolationLevel.Value;
            using (var transactionScope = (disableTransactionScope || !autoCommit || SqlCommand == SqlCommand.Truncate) ? null :
                new TransactionScope(TransactionScopeOption.Required, new TransactionOptions() { IsolationLevel = isolationLevel }))
            {
                if (list != null)
                {
                    List = list;
                }
                var q = new Sql<T>()
                {
                    SqlCommand = SqlCommand
                };
                using (var connection = GetSqlDatabase().Open())
                {
                    for (int i = 0; i < List.Count; i++)
                    {
                        q.Data = List[i];
                        if ((!autoCommit || SqlCommand == SqlCommand.Truncate) && Transaction.Current == null)
                        {
                            SqlError = "Transaction scope must be provided for manual commit";
                        }
                        else
                        {
                            q.RunSaveSQL(connection);
                        }
                        q.ResolveSQL();
                        if (q.Errors.Count > 0)
                        {
                            break;
                        }
                    }
                }
                if (transactionScope != null && q.SqlError == "" && q.Count > 0 && q.Result > 0 &&
                    q.Count == q.Result && q.Errors.Count == 0)
                {
                    transactionScope.Complete();
                }
            }
            return this;
        }

        public Sql<T> SaveList(List<T> list = null)
        {
            if (SqlCommand != SqlCommand.Insert || SqlCommand != SqlCommand.Update || SqlCommand != SqlCommand.Delete || SqlCommand != SqlCommand.Truncate)
            {
                SqlCommand = SqlCommand.Replace;
            }
            return QSaveList(list);
        }

        public Sql<T> InsertList(List<T> list = null)
        {
            SqlCommand = SqlCommand.Insert;
            return QSaveList(list);
        }

        public Sql<T> UpdateList(List<T> list = null)
        {
            SqlCommand = SqlCommand.Update;
            return QSaveList(list);
        }

        public Sql<T> DeleteList(List<T> list = null)
        {
            SqlCommand = SqlCommand.Delete;
            return QSaveList(list);
        }

        private Sql<dynamic> BuildJoinSQL(string joinType, List<ISql> sqlList, List<Criteria> joinCols)
        {
            var sqlDynamic = new Sql<dynamic>();
            ISqlDatabase sqlDatabase = GetSqlDatabase();
            string fields = "";
            string joins = "";
            for (int i = 0; i < sqlList.Count; i++)
            {                
                if (sqlList[i].RootTable != "" && sqlList[i].TableFieldList.Count == 0)
                {
                    ((dynamic)sqlList[i]).SqlCommand = SqlCommand.Select;
                    ((dynamic)sqlList[i]).BuildSQL(false, true);
                    sqlList[i].TableFieldList.Add(new Criteria() { Table = sqlList[i].RootTable });                   
                    if (i > 0 || sqlList.Count == 1)
                    {
                        if (joinCols.Count == 0)
                        {
                            //Match column between previous table and current table and use it as join column
                            IEnumerable<PropertyInfo> prevProperties = new List<PropertyInfo>().AsEnumerable();
                            IEnumerable<PropertyInfo> properties = new List<PropertyInfo>().AsEnumerable();
                            try
                            {
                                prevProperties = ((dynamic)sqlList[i - 1]).Data.GetType().GetProperties();
                                properties = ((dynamic)sqlList[i]).Data.GetType().GetProperties();
                            }
                            catch
                            {

                            }
                            var property = properties.Where(x => prevProperties.Any(y => y.Name.ToLower() == x.Name.ToLower())).FirstOrDefault();
                            if (property != null)
                            {
                                sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i - 1].RootTable, Column = property.Name, Operator = "=" });
                                sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i].RootTable, Column = property.Name });
                            }
                        }
                        else if (joinCols.Count == 1)
                        {
                            if (sqlList.Count == 1)
                            {
                                var criteria = sqlDynamic.TableFieldList[sqlDynamic.TableFieldList.Count - 1];
                                sqlList[i].TableFieldList.Add(new Criteria { Table = criteria.Table, Column = criteria.Column, Operator = "=" });
                                sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i].RootTable, Column = joinCols[0].Column });
                            }
                            else
                            {
                                sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i - 1].RootTable, Column = joinCols[0].Column, Operator = "=" });
                                sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i].RootTable, Column = joinCols[0].Column });
                            }                            
                        }
                        else if (joinCols.Count == 2)
                        {
                            sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i - 1].RootTable, Column = joinCols[0].Column, Operator = "=" });
                            sqlList[i].TableFieldList.Add(new Criteria { Table = sqlList[i].RootTable, Column = joinCols[1].Column });
                        }
                    }
                }
                if (sqlList[i].TableFieldList.Count > 0)
                {
                    if (sqlList[i].RootFieldList.Count == 0)
                    {
                        sqlDynamic.RootFieldList.Add(new Criteria() { Table = sqlList[i].RootTable, Column = "*" });
                    }
                    else
                    {
                        sqlDynamic.RootFieldList.AddRange(sqlList[i].RootFieldList);
                    }                    
                    sqlDynamic.TableFieldList.AddRange(sqlList[i].TableFieldList);
                }
            }           
            if (sqlDynamic.RootFieldList.Count == 0)
            {
                fields = "*";
            }
            else
            {
                for (int f = 0; f < sqlDynamic.RootFieldList.Count; f++)
                {
                    bool skipGetSQLEscapeName = sqlDynamic.RootFieldList[f].Table.Contains("information_schema");
                    string sqlTable = skipGetSQLEscapeName ? sqlDynamic.RootFieldList[f].Table : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, sqlDynamic.RootFieldList[f].Table);
                    string sqlField = skipGetSQLEscapeName ? sqlDynamic.RootFieldList[f].Column : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, sqlDynamic.RootFieldList[f].Column);
                    if (sqlDynamic.RootFieldList[f].Column == "*")
                    {
                        fields += sqlTable + ".*,";
                    }
                    else
                    {
                        fields += sqlTable +
                            "." + sqlField + ",";
                    }
                }
                fields = fields.TrimEnd(',');
            }
            for (int t = 0; t < sqlDynamic.TableFieldList.Count; t++)
            {
                bool skipGetSQLEscapeName = sqlDynamic.TableFieldList[t].Table.Contains("information_schema");
                string sqlTable = skipGetSQLEscapeName ? sqlDynamic.TableFieldList[t].Table : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, sqlDynamic.TableFieldList[t].Table);
                string sqlField = skipGetSQLEscapeName ? sqlDynamic.TableFieldList[t].Column : Api.GetSQLEscapeName(sqlDatabase.DatabaseProvider, sqlDynamic.TableFieldList[t].Column);
                if (sqlDynamic.TableFieldList[t].Column == "")
                {
                    //JOIN table
                    if (joins == "")
                    {
                        joins = sqlTable;
                    }
                    else
                    {
                        joins += " " + joinType + " " + sqlTable + " ON ";
                    }
                }
                else
                {
                    //JOIN column
                    joins += sqlTable + "." +
                        sqlField +
                        (sqlDynamic.TableFieldList[t].Operator ?? "");
                }
            }
            if (sqlDatabase.DatabaseProvider == DatabaseProvider.MySQL && joinType.Contains("FULL JOIN"))
            {
                sqlDynamic.SQL = "SELECT " + fields + " FROM " + joins.Replace(" FULL JOIN ", " LEFT JOIN ") +
                    " UNION " +
                    "SELECT " + fields + " FROM " + joins.Replace(" FULL JOIN ", " RIGHT JOIN ");
            }
            else
            {
                sqlDynamic.SQL = "SELECT " + fields + " FROM " + joins;
            }
            return sqlDynamic;
        }

        private Sql<dynamic> Join(string joinType, ISql sql, List<Criteria> joinCols)
        {            
            return BuildJoinSQL(joinType, new List<ISql> { this, sql }, joinCols);
        }

        private Sql<dynamic> Join(string joinType, List<ISql> sqlList, List<Criteria> joinCols)
        {            
            return BuildJoinSQL(joinType, sqlList, joinCols);
        }

        public Sql<dynamic> InnerJoin(ISql sql, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("INNER JOIN", sql, joinCols);
        }

        public Sql<dynamic> InnerJoin(ISql sql, string fromColumn, string toColumn)
        {
            var joinCols = new List<Criteria>
            {
                new Criteria { Column = fromColumn, Operator = "=" },
                new Criteria { Column = toColumn }
            };
            return Join("INNER JOIN", sql, joinCols);
        }

        public Sql<dynamic> InnerJoin(List<ISql> sqlList, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("INNER JOIN", sqlList, joinCols);
        }

        public Sql<dynamic> LeftJoin(ISql sql, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("LEFT JOIN", sql, joinCols);
        }

        public Sql<dynamic> LeftJoin(ISql sql, string fromColumn, string toColumn)
        {
            var joinCols = new List<Criteria>
            {
                new Criteria { Column = fromColumn, Operator = "=" },
                new Criteria { Column = toColumn }
            };
            return Join("LEFT JOIN", sql, joinCols);
        }

        public Sql<dynamic> LeftJoin(List<ISql> sqlList, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("LEFT JOIN", sqlList, joinCols);
        }

        public Sql<dynamic> RightJoin(ISql sql, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("RIGHT JOIN", sql, joinCols);
        }

        public Sql<dynamic> RightJoin(ISql sql, string fromColumn, string toColumn)
        {
            var joinCols = new List<Criteria>
            {
                new Criteria { Column = fromColumn, Operator = "=" },
                new Criteria { Column = toColumn }
            };
            return Join("RIGHT JOIN", sql, joinCols);
        }

        public Sql<dynamic> RightJoin(List<ISql> sqlList, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("RIGHT JOIN", sqlList, joinCols);
        }

        public Sql<dynamic> FullJoin(ISql sql, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("FULL JOIN", sql, joinCols);
        }

        public Sql<dynamic> FullJoin(ISql sql, string fromColumn, string toColumn)
        {
            var joinCols = new List<Criteria>
            {
                new Criteria { Column = fromColumn, Operator = "=" },
                new Criteria { Column = toColumn }
            };
            return Join("FULL JOIN", sql, joinCols);
        }

        public Sql<dynamic> FullJoin(List<ISql> sqlList, string onColumn = null)
        {
            var joinCols = new List<Criteria>();
            if (!string.IsNullOrEmpty(onColumn))
            {
                joinCols.Add(new Criteria { Column = onColumn });
            }
            return Join("FULL JOIN", sqlList, joinCols);
        }

        internal void ResolveSQL()
        {
            if (SQL != "")
            {
                bool isModifyCommand = SqlCommand == SqlCommand.Insert || SqlCommand == SqlCommand.Update ||
                    SqlCommand == SqlCommand.Delete || SqlCommand == SqlCommand.Replace;
                if (SqlError == "")
                {
                    if (isModifyCommand)
                    {
                        if ((Count > 0 || Result > 0) && Count != Result)
                        {
                            SqlError = "Database transaction check error: count=" + Count + ", result=" + Result;
                        }
                        if (Count == 0 && Result == 0)
                        {
                            SqlError = "No record affected";
                        }
                    }
                }
            }
            if (SqlError != "")
            {
                if (SQL != "")
                {
                    SqlError += ", SQL=" + SQL;
                }                
                Errors.Add(new Error((RootTable == "" ? "Sql" : RootTable) + ": " + SqlCommand.ToString(), SqlError));
            }
            if (Errors.Count > 0)
            {
                if (SqlFactory.EnableTextFileLogging)
                {
                    Api.WriteLog(Api.GetInternalErrorLog(Errors));
                }
                else
                {
                    throw new Exception(Api.GetInternalErrorLog(Errors));
                }
            }
        }
    }
}
