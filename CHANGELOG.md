# 3.6.0
- Fix bug when multiple update expressions are included without separated by comma, e.g.:
```csharp
UPDATE product SET balance=balance-3price=price*2 WHERE product_id = @product_id
```
# 3.5.0
- Add guard to prevent UPDATE/DELETE table without condition set, i.e. allowable only via manual commit, which same as TRUNCATE command
- Fix bug by removing unnecessary assignment of list data after saving data

# 3.4.0
- Fix a bug that always reuse first nested child data when inserting

# 3.3.0
- Add new aggregate method `GetDistinctCount`

# 3.2.0
- Add support for chaining multiple values of `AND` logic
- Fix an issue not closing `ORDER BY` cases

# 3.1.0
- Change method `GetCount` to initialize another instance to select count with existing instance `WHERE` conditions so that existing conditions will not be removed or changed, it's quite common you will need this for paging purpose
- Add extra check to prevent logic phrase `AND`/`OR` before a bracket

# 3.0.0
- Add handle to convert to lower case when method `GetSQLEscapeName` is called, this actually cause problem on selecting from table

# 2.9.0
- Add method `GetDynamicSingle` for non-strongly type query purpose, `null` will be returned if more than 1 element found from query, use `GetDynamicList` if you would like a `List` returned instead

# 2.8.0
- Add `DisableTransactionScope`, `AutoCommit`, `IsolationLevel` option support per class instance, default option settings from `SqlFactory` if options not set for a `Sql<T>` class instance
- Add feature to update non-id null child field values from match parent fields with values, in most cases link between 2 tables usually bind to one key, but there are cases where data structure is not normalized, e.g.:

Assume a customer order has many seller order records, it's is clear now that we have duplicated data `order_no` in child table `seller_order`:
```csharp
public class Customer_Order 
{
	public int? customer_order_id { get; set; }
	public string order_no { get; set; }
	public List<Seller_Order> seller_order_list { get; set; } = new List<Seller_Order>();
}

public class Seller_Order
{
	public int? seller_order_id { get; set; }
	public int? customer_order_id { get; set; }
	public string order_no { get; set; }
}
```
When we insert a customer order:
```csharp
var new_customer_order = new Customer_Order(); //Assign whatever data neccessary except auto-increment key, which is populated automatically after insert, but non-id field like `order_no` is not based on previous logic handling

var customerOrderQ = new Sql<Customer_Order>().Insert(customer_order);
```
In this case, the field `order_no` from parent table `Customer_Order` will be populated (if not null) to child table `Seller_Order` if the child field value is null

- Add feature to allow update column via expression in string[] format, if null or empty string is provided column name will be used, e.g.:
```csharp
var q = new Sql<User>()
	 .IncludeColumn("QQ")
    .IncludeColumn("Balance", new[] { "", "-", "1" }).ToUpdate(new User { UserId = 3, QQ = "abc" });
```
# 2.7.0
- Add more checks for `WHERE` methods when different array type data is provided include `List`, `IEnumerable`, or [], ensure you provide only valid array data
- Add `OrderByCase` and `OrderByCases` method, note that you can provide cases you need in string format, e.g.:
```csharp
 var q = new Sql<Product_Description>()
    .IncludeColumn("name")
    .And("name", "like", "milo")
    .OrderByCases(new[] { "when name like 'milo%' then 0", "else 1" }).OrderBy("name").Limit(20);
```
# 2.6.0
- Rewrite all `WHERE` methods due to parameters conflicts when column has type `int`, which confuse with enum type like `Operator`, all operator input now will be string sign, e.g.
```csharp
"=" (equal or case insensitive equal)
"<>" (not equal or case insensitive not equal)
"==" (case sensitive equal)
"!=" (case sensitive not equal)
"is void" (null/zero/false/empty string after trim)
```
- For multiple conditions query with different operators, an operator sign must be provided, the following signs are supported:
{ "=", "<>", "==", "!=", "<", ">", "<=", ">=", "is", "is not", "like", "is void" }
Example:
```csharp
var customers_active_today = new Sql<Customer>()
	.And("last_loggedon", ">=", todayStartDt).And("last_loggedon", "<=", todayEndDt);
```
- For normal equal comparison, consider using strong type or dynamic/anonymous data as input, 
Example:
```csharp
var customer = new Sql<Customer>().GetSingle(new Customer { customer_id = 3 });
var customer = new Sql<Customer>().GetSingle(new { customer_id = 3 });
```
- Simplify bracket and logic chaining and add support for multiple values of `OR` logic:

Longer code:
```csharp
var q = new Sql<User>()
            .OpenBracket().Or("UserId", "=", 3).Or("UserId", ">=", 5).CloseBracket()
            .And()
            .OpenBracket().Or("UserGroupId", "=", 1).Or("UserGroupId", "=", 6).Or("UserGroupId", "=", 7).CloseBracket()
```
Shorter code:
```csharp
var q = new Sql<User>()
            .OpenBracket().Or("UserId", "=", 3).Or("UserId", ">=", 5).CloseBracket()
            .And()
            .OpenBracket().Or("UserGroupId", "=", new[] { 1, 6, 7 }).CloseBracket()
```
# 2.5.0
- Fix automatic set of `WHERE` condition for nested generic object, which cannot be used as parameter for SQL query

# 2.4.0
- Improve method `GetSingle` to return null value if no record found
- Improve method `GetCount` to accept data values (strong type or dynamic/anonymous) for `WHERE` conditions, other methods like `Select` & `GetList` work in a similar way, do remember that try avoid to reuse the same query for write purpose, conditions overriding or conflicts can cause unwanted effects
- Change method `WhereTable(string table, string column, Operator _operator, dynamic value, Logic logic = Logic.And)` for public access
- Add more method signatures for `WHERE` conditions with `Bracket.Open`, `Logic.Or`, `Bracket.Close`

# 2.3.0
- Fix bug where `Count`,`Result`,`SqlError`,`Errors` is not being reset when executing core method `QSave`, resulting incorrect count check for write query commit if previous query is to read count, thus it is always recommended to adhere to the CQRS strategy unless you know what you are doing
- Fix nested object data check error when no data returned to be assigned to `List`

# 2.2.0
- Add support to read & write nested generic property as previously only support nested generic `List` property, in many case it's common to have 1 to 1 mapping between parent and child object, and we do not have to use `List` every time as accessing `List` index can be extra work

# 2.1.0
- Fix `CriteriaList` assign by reference problem, which causing the list to keep accumulate data, resulting incorrect nested data criteria checking

# 2.0.0
- Fix `Select` bug for nested class data
- Remove automatic `WHERE` conditions set when dynamic data provided for string table query (not strongly type data)

# 1.9.0
- Fix `isChild` flag not passing into function `RunSaveSQL`, as a result child list data will not be inserted successfully
- Fix `SqlError` checking bug after SQL execution, basically replace most `null` check with empty string, this checking bug will result unexpected error when resolving SQL

# 1.8.0
- Fix `GetCount` bug when `WHERE` conditions are not take into account
- Fix strong type detection via `QSetData` when class initialization with database configuration does not, resulting in empty `RootTable`

# 1.7.0
- Remove case insensitive handling for `information_schema` tables & columns handling, so by default it is case sensitive, ensure you provide correct casing in case you need the database info
- Fix bug for method `GetList()` where `List` should not contain a single instance data when result is 0 record, if a class you defined does not belong to any table, instead the class is consists of list of table classes, then an instance will be added `List` and all lists' data will be selected, this maybe useful if you would like to group master data in a batch

# 1.6.0
- Change parameter type from `List<string>` to `string[]` for almost all methods
- Add word `Table` for table specific WHERE methods, e.g. WhereTable, WhereTableOr, WhereTableOpenBracket, the problem indeed is due to parameters overloading conflict, it happens when it is not possible to differentiate which WHERE method signature is called when 2 strings are provided, e.g. assume you would like to select column `column_type` with string value `BASE TABLE`, in this case the wrong method signature could be called WHERE the condition is `table` = "column_type" and `column` = "BASE TABLE"
- Fix bug for method `Sql<T>.Configure` where class is re-initialized which will reset all class data like WHERE/GROUP BY/ORDER BY/pipe conditions
- Add a new method `SqlFactory.GenerateCSFiles`, classes will be generated based on parameters: sqlDatabase, classNamespace, and path, currently only support Microsoft SQL Server & MySQL `information_schema`

# 1.5.0
- Add `FULL JOIN` support for `MySQL` (with combinations of LEFT JOIN, RIGHT JOIN, UNION)
- Fix few JOIN & aggregation bugs
- Rename aggregation methods, note that you have to provide table name if ambiguous column name included in the query

# 1.4.0
- Remove few criteria column classes include IdColumn, PipeColumn, WhereColumn, and SortColumn
- Rename SortBy method to OrderBy
- Combine column classes deprecated above into `List<Criteria>`
- Remove "child" word for all Include/Exclude/Where/GroupBy/OrderBy/Limit methods,
- Table name now can be provided to indicate more accurately where a column belong to, if no table name provided it is considered as root table, i.e. parent table
- Remove all validations for all Include/Exclude/Aggregate/GroupBy/OrderBy/Limit methods (take care what parameter you input!)
- Add error prompt when no transaction scope is provided for manual commit (`AutoCommit = false`)
- `TRUNCATE` command now need to commit manually as it is too dangerous to chop data directly, with the exception that you disable transaction scope, all data will be written directly without transaction control, make sure you know what you're doing!
- Transaction control has been fixed for `Sqlite3`
- `JOIN` support for multiple tables, it's recommended that strongly typed classes with matching key columns defined are used for joins
