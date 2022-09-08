# WhizQ - A fluent API for SQL database, excellent playground for C# OOP

## Dependencies
1. **Dapper** - One of the best micro ORM
2. **Jil** - One of the fastest JSON serializer, although I prefer `Deserializer` (more compatible) & `Formatting.Indented` presentation from **Newtonsoft.Json**
3. **Sodium** - Compact encryption package
4. Database drivers: `System.Data.SqlClient`, `MySql.Data.MySqlClient`, `NpgSql`, `System.Data.Sqlite`

## Usage
**WhizQ** can be used to build and return strongly typed/dynamic SQL objects/data/list, 4 databases are supported: Microsoft SQL Server, MySQL, PostgreSQL, and SQLite3

To configure database, you can refer to the following settings:

## I) Factory setting
1. Passing in database provider and connection string, other settings available include `DisableTransactionScope`, `IsolationLevel`, `AutoCommit`, `EnableTextFileLogging`:
```csharp
var connectionString = @"Server=localhost\SQLEXPRESS;Database=WhizWorkshop;User Id=whiz;Password=whiz19830312;";
SqlFactory.Configure(DatabaseProvider.MicrosoftSQLServer, connectionString);
```
2. Provide a class that implement interface `ISqlConfig` or `ISqlDatabase`
```csharp
public class Config : ISqlConfig
{
  public DatabaseProvider DatabaseProvider { get; internal set; } = DatabaseProvider.MicrosoftSQLServer;
  public string ConnectionString { get; internal set; }
  public string HangfireConnectionString { get; internal set; }
  ...
}

SqlFactory.Configure(new Config());
```
3. `SqlDatabase` is default implementation built in for interface `ISqlDatabase`, you can implement your own `Open` method, e.g. `AvatarDatabase`:
```csharp
public class SqlDatabase : ISqlDatabase
{
  ...
  public IDbConnection Connection { get; set; }
  
  public IDbConnection Open()
  {
    if (DatabaseProvider == DatabaseProvider.MicrosoftSQLServer)
    {
      Connection = new SqlConnection(ConnectionString);
    }
    ...
    return Connection;
  }
}

public class AvatarDatabase : ISqlDatabase
{
  ...
  public IDbConnection Connection { get; set; }
  
  public IDbConnection Open()
  {
    if (DatabaseProvider == DatabaseProvider.PostgreSQL)
    {
      Connection = new AvatarConnection(ConnectionString);
    }
    ...
    return Connection;
  }
}

SqlFactory.Configure(new AvatarDatabase(DatabaseProvider.PostgreSQL, connectionString);
```
## II) Class instance setting
1. Configure via class inialization or call method `Configure` will ignore factory setting:
```csharp
var q = new Sql<User>(new Config());
var users = q.GetList();

var q2 = new Sql<User>().Configure(DatabaseProvider.MicrosoftSQLServer, connectionString);
var user = q2.GetSingle(new { UserId = 3 });
```
## How to read data?
1. Create a class with public access properties, class name must be same as your table name, assume tables User, UserToken, UserBankCard are linked via key field `UserId`, default matching logic is first key field with same name from 2 tables, e.g.:
```csharp
public class User
{
  public int? UserId { get; set; }
  public string UserName { get; set; }
  ...
  public List<UserToken> UserTokenList { get; set; } = new List<UserToken>();
  public List<UserBankCard> UserBankCardList { get; set; } = new List<UserBankCard>();
}
```
Note that when method name in plural like `IncludeColumns` you will need to provide array data, where singular method like `IncludeColumn` is always refer to single column, or column of a table:
```csharp
//Include column `UserName` from table `User`, thus do not misunderstand it's array data you provided in this case
//It's 2 parameters (table & column)
var q = new Sql<object>("User").IncludeColumn("User", "UserName");

//Include columns based on array data provided
var q = new Sql<User>().IncludeColumns(new[] { "UserName", "DateTimeModified", "VIPPoint" });
```
2. Return `Sql` object or query data (single or multiple rows include nested data linked by same property field name between parent and child class) using the API:
```csharp
//This will set conditions and return Sql object only, no action taken
var q = new Sql<User>(new User { UserId = 3 });

//Set id value and select field `UserName` only, id field will always be included
q.SetData(new { UserId = 5 }).IncludeColumn("UserName");

//This will override all previous field actions, thus User, UserToken, UserBankCard fields will be returned
//Except field `PasswordHash`
q.ExcludeColumn("PasswordHash");

//Set condition for UserId
q.And("UserId", "=", 3);

//Condition for string casing select
q.And("UserName", "=", "admin"); //Case insensitive equal
q.And("UserName", "==", "admin");  //Case sensitive equal

//Return data from table User, UserToken, UserBankCard into User object
//Note that result will always be returned to `List`, while `Data` will be used as input parameter
var user = q.Select().List[0];
var user = q.GetSingle();
var userList = q.GetList();

//`Data` is used for input purpose only
var inputData = q.Data;

//Select top 10 users sort by `UserName`, page number can be provided with the `Limit` method too
var users = new Sql<User>().OrderBy("UserName").Limit(10).Select().List;
//OR
var users = new Sql<User>().OrderBy("UserName").Limit(10).GetList();

//Count of total users
var userCount = new Sql<User>().GetCount();

//Conditions with OR
var q2 = new Sql<User>().Or(new[] { "UserName", "Email", "PhoneNo" }, "=", userIdStr).Select();

//Chaining conditions
var q3 = new Sql<User>(new User()).IncludeColumns(new[] { "UserId", "UserName",
  "DateTimeLastLoggedIn", "DateTimeModified", "AgentUserId" })
  .IncludeColumn("UserToken", "DateTimeModified").And("UserId", user.UserId);

//Showing top 10 latest comments from all active topics
//Assume `Comment` is child table link to parent table `Topic` via same key `TopicId
var lst = new Sql<Topic>().Limit("Comment", 10).And("IsActive", "=", true)
  .OrderByDesc("Comment", "DateTimeCreated").Select().List;

//Return dynamic report list of user group count with count column alias name `User Group Count`
var summarylst = new Sql<User>().AddCount("", "User Group Count").GroupBy("UserGroupId").GetDynamicList();

//Tables JOIN
var userQ = new Sql<User>().IncludeColumns(new[] { "Email", "FullName" });
var userTokenQ = new Sql<UserToken>().IncludeColumns(new[] { "Token", "DateTimeModified" });
var userBankCardQ = new Sql<UserBankCard>().IncludeColumns(new[] { "BankId", "AccountName", "BankCardNo" });

var joinQ = userQ.FullJoin(userTokenQ, "UserId");
var d = joinQ.GetList();
        
var joinQ2 = userQ.RightJoin(userBankCardQ, "UserId");  
var d2 = joinQ2.GetList();

//JOIN with aggregations
var joinQ = userQ.FullJoin(userTokenQ, "UserId")
  .AddCount().AddTableMin("User", "UserId").AddMax("VIPPoint")
  .GroupBy("UserGroupId")
  .Limit(2);

//The difference between a strong type list and dynamic list with aggregations
var q = new Sql<User>().AddMax("VIPPoint").Where("UserGroupId", 3).GroupBy("UserName").OrderBy("UserName").Limit(5);
var strongTypeList = q.GetList(); //return empty List<User> when aggregations involved
var dynamicList = q.GetDynamicList(); //return List<dynamic> with aggregations result
```
## How to write data?
1. Call method `Save` and provide data as parameter:
```csharp
dynamic data; //http response data to be returned to Web frontend
var q = new Sql<User>();
User user = q.GetSingle(new User { UserId = 3 });
AccountApi.RenewToken(ref user, origin);
user.DateTimeLastLoggedIn = DateTime.UtcNow;
if (q.IncludeColumn("DateTimeLastLoggedIn").IncludeTable("UserToken").Save(user).Errors.Count == 0)
{
    data = AccountApi.ExcludeSensitiveData(user);
}
```
2. How about transactions for multiple tables? Refer to the following `SqlFactory` code where admin generate a link token for new member registration, at the same time the admin token will be renewed when transactions are committed:
```csharp
dynamic data; //http response data to be returned to Web frontend
admin.DateTimeModified = DateTime.UtcNow;
AccountApi.RenewToken(ref admin, origin);
var adminQ = new Sql<User>(admin).IncludeColumn("DateTimeModified").IncludeTable("UserToken");
var memberQ = new Sql<UserToken>(AccountApi.GenerateLinkToken(user.UserId.Value, member, origin));
if (SqlFactory.Save(new List<ISql> { adminQ, memberQ }).Select(x => x.Errors.Count).Sum() == 0)
{
    dynamic adminData = AccountApi.ExcludeSensitiveData(admin);
    data = new { adminData.User, adminData.Token, LinkToken = memberQ.Data.Token };
}
```
3. Data can be committed manually by disable `AutoCommit` setting of `SqlFactory`:
```csharp
SqlFactory.AutoCommit = false;
var bankQ = new Sql<Bank>(new Bank { BankId = 36, BankCode = "TEST", BankName = "Test" }).SetManualId("BankId").ToInsert();
var noticeQ = new Sql<Notice>(new Notice { Title = "Notice", Content = "Annoucement News!!!", DateTimeCreated = DateTime.Now }).ToInsert();
using (var transactionScope = new TransactionScope())
{
    if (SqlFactory.Save(new List<ISql> { bankQ, noticeQ }).Select(x => x.Errors.Count).Sum() == 0)
    {
        transactionScope.Complete();
    }
}
```
4. TRUNCATE command by default need to be committed manually using your own transaction scope or you can set `DisableTransactionScope = false` to write data directly, make sure you know what you're doing with no transaction control!
```csharp
using (var transactionScope = new TransactionScope())
{
    new Sql<Notice>().Truncate();
    transactionScope.Complete();
}
```
## Features to be added in future (or have been solved)?
- [x] Support of dynamic query without generic type object being defined, input data can be Anonymous/ExpandoObject type, also custom table & column strings will be used to form query
- [ ] Database schema creation and migration plan
- [ ] Generate classes from database schema, i.e. tables & columns
- [ ] Prepare & construct SQL statement without actual execution
- [ ] Stored procedure support
- [ ] Composite primary key, current methods supported are `SetAutoIncrementId` and `SetManualId` to set a single primary key per table
- [ ] Custom link key(s) between parent and child table, current logic is search for first matched Id property between parent class and child list class
- [x] Custom settings include `DisableTransactionScope`(false), `IsolationLevel`(ReadCommitted), `AutoCommit`(true), `EnableTextFileLogging`(false)
- [x] Transaction scope is suppressed for `Sqlite3` database due to locking issue during read/write operation, i.e. there is no transaction rollback control yet for `Sqlite3`
- [ ] Asynchronous and multithreading parallel tasks support 
- [x] GROUP BY along with aggregation functions COUNT, MIN, MAX, SUM, AVG are not supported yet
- [x] JOIN between tables
- [ ] NoSQL integration
