using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

using Jil;
using Sodium;

namespace WhizQ
{
    public enum Bracket
    {
        None,
        Open,
        Close
    }

    public enum DatabaseProvider
    {
        MySQL,
        MicrosoftSQLServer,
        PostgreSQL,
        SQLite3
    }
    public enum ErrorType
    {
        Internal,
        Public
    }

    public enum Logic
    {
        None,
        And,
        Or,
    }

    public enum Pipe
    {
        None,
        Include,
        Exclude,
        Limit,
        Count,
        DistinctCount,
        Min,
        Max,
        Sum,
        Average
    }

    public enum SortOrder
    {
        None,
        Asc,
        Desc
    }

    public enum SqlCommand
    {
        Skip,
        Select,
        Insert,
        Update,
        Delete,
        Replace,
        Truncate
    }

    public enum StringFormat
    {
        Number,
        Upper,
        Lower,
        Letter,
        Symbol,
        AlphaNumeric,
        Mixed,
    }

    public class Api
    {
        public static string BytesToHex(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder(bytes.Length * 2);
            foreach (byte b in bytes)
                sb.AppendFormat("{0:x2}", b);
            return sb.ToString();
        }

        public static BigInteger? ComNR(int n, int r)
        {
            //Combinations count of arranging n items into r items, exclude repetition where AB is same as BA
            if (n < 1 || r < 1 || r > n)
            {
                return null;
            }
            else
            {
                return Factorial(n) / (Factorial(r) * Factorial(n - r));
            }
        }

        public static dynamic CreateGenericListInstance(Type listType)
        {
            //Get list instance of custom class generic type (not a System type)
            dynamic listInstance = null;
            if (IsGenericListType(listType))
            {
                listInstance = (IList)typeof(List<>).MakeGenericType(listType.GenericTypeArguments[0]).GetConstructor(Type.EmptyTypes).Invoke(null);
                dynamic instance = Activator.CreateInstance(listType.GenericTypeArguments[0]);
                if (instance != null)
                {
                    List<PropertyInfo> properties = new List<PropertyInfo>(instance.GetType().GetProperties());
                    for (int i = 0; i < properties.Count; i++)
                    {
                        if (IsGenericListType(properties[i].PropertyType))
                        {
                            properties[i].SetValue(instance, CreateGenericListInstance(properties[i].PropertyType));
                        }
                    }
                }
                listInstance.Add(instance);
            }
            return listInstance;
        }

        public static BigInteger? Factorial(int n)
        {
            BigInteger f = 1;
            if (n < 0)
            {
                return null;
            }
            else if (n < 3)
            {
                return n;
            }
            else
            {
                for (int i = 2; i <= n; i++)
                {
                    f *= i;
                }
                return f;
            }
        }

        public static BigInteger? FactorialP(int n)
        {
            return n < 3 ? Factorial(n) : Enumerable.Range(2, n - 1).AsParallel().Aggregate(BigInteger.One, (f, i) => f * i);
        }

        public static BigInteger? FactorialR(int n)
        {            
            return n < 3 ? Factorial(n) : Factorial(n - 1) * n;
        }

        public static string GetInternalErrorLog(List<Error> errors)
        {
            StringBuilder sb = new StringBuilder();
            errors = errors.Where(x => x.ErrorType == ErrorType.Internal).ToList();
            for (int i = 0; i < errors.Count; i++)
            {
                Error error = errors[i];

                string newLine = Environment.NewLine;
                if (i == errors.Count - 1)
                {
                    newLine = "";
                }
                if (error.DateTimeLogged == null)
                {
                    error.DateTimeLogged = DateTime.UtcNow;
                }
                sb.Append(error.DateTimeLogged.Value.ToString("yyyy-MM-dd HH:mm:ss") + " [" + error.Title + "] " + error.Description + newLine);

                if (error.Data != null)
                {
                    sb.Append(", data=" + Environment.NewLine + JSON.Serialize(error.Data, Options.PrettyPrint));
                }
            }
            return sb.ToString();
        }

        public static PhysicalAddress GetMacAddress()
        {
            foreach (NetworkInterface nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                // Only consider Ethernet network interfaces
                if (nic.NetworkInterfaceType == NetworkInterfaceType.Ethernet &&
                    nic.OperationalStatus == OperationalStatus.Up)
                {
                    return nic.GetPhysicalAddress();
                }
            }
            return null;
        }

        public static string GetMd5Hash(string str)
        {
            using (MD5 md5 = MD5.Create())
            {
                byte[] data = md5.ComputeHash(Encoding.UTF8.GetBytes(str));
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < data.Length; i++)
                {
                    sb.Append(data[i].ToString("x2"));
                }
                return sb.ToString();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public static string GetMethodName()
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrame(1);
            return sf.GetMethod().Name;
        }

        public static string GetSQLEscapeName(DatabaseProvider databaseProvider, string name)
        {
            if (databaseProvider == DatabaseProvider.MicrosoftSQLServer || databaseProvider == DatabaseProvider.SQLite3)
            {
                name = "[" + name.ToLower() + "]";
            }
            else if (databaseProvider == DatabaseProvider.MySQL)
            {
                name = "`" + name.ToLower() + "`";
            }
            else if (databaseProvider == DatabaseProvider.PostgreSQL)
            {
                name = "\"" + name.ToLower() + "\"";
            }
            return name;
        }

        public static string GetRandomString(int length, StringFormat[] lst)
        {
            StringBuilder sb = new StringBuilder();
            string keyspace = "";
            if (lst.Any(x => x == StringFormat.Number))
                keyspace = "0123456789";

            if (lst.Any(x => x == StringFormat.Letter))
            {
                keyspace += "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            }
            else
            {
                if (lst.Any(x => x == StringFormat.Upper))
                    keyspace += "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                if (lst.Any(x => x == StringFormat.Lower))
                    keyspace += "abcdefghijklmnopqrstuvwxyz";
            }

            if (lst.Any(x => x == StringFormat.Symbol))
                keyspace += "~!@#$%^&*()_+`-={}|[]\\:\";'<>?,./";

            for (int i = 0; i < length; i++)
            {
                sb.Append(keyspace[SodiumCore.GetRandomNumber(keyspace.Length)]);
            }
            return sb.ToString();
        }

        public static string GetRandomString(int length, StringFormat stringFormat = StringFormat.AlphaNumeric)
        {
            if (stringFormat == StringFormat.AlphaNumeric)
            {
                return GetRandomString(length, new[] { StringFormat.Letter, StringFormat.Number });
            }
            else if (stringFormat == StringFormat.Mixed)
            {
                return GetRandomString(length, new[] { StringFormat.Letter, StringFormat.Number, StringFormat.Symbol });
            }
            else
            {
                return GetRandomString(length, new[] { stringFormat });
            }
        }

        public static string GetTimeAgo(DateTime datetime, bool isUTC = true)
        {
            string timeAgo;
            TimeSpan timeSince;
            if (isUTC)
            {
                timeSince = DateTime.UtcNow.Subtract(datetime);
            }
            else
            {
                timeSince = DateTime.Now.Subtract(datetime);
            }
            if (timeSince.TotalMinutes < 1)
                timeAgo = "just now";
            else if (timeSince.TotalMinutes < 2)
                timeAgo = "1 minute ago";
            else if (timeSince.TotalMinutes < 60)
                timeAgo = string.Format("{0} minutes ago", timeSince.Minutes);
            else if (timeSince.TotalMinutes < 120)
                timeAgo = "1 hour ago";
            else if (timeSince.TotalHours < 24)
                timeAgo = string.Format("{0} hours ago", timeSince.Hours);
            else if (timeSince.TotalDays == 1)
                timeAgo = "yesterday";
            else if (timeSince.TotalDays < 7)
                timeAgo = string.Format("{0} days ago", timeSince.Days);
            else if (timeSince.TotalDays < 14)
                timeAgo = "last week";
            else if (timeSince.TotalDays < 21)
                timeAgo = "2 weeks ago";
            else if (timeSince.TotalDays < 28)
                timeAgo = "3 weeks ago";
            else if (timeSince.TotalDays < 60)
                timeAgo = "last month";
            else if (timeSince.TotalDays < 365)
                timeAgo = string.Format("{0} months ago", Math.Round(timeSince.TotalDays / 30));
            else if (timeSince.TotalDays < 730)
                timeAgo = "last year";
            else
                timeAgo = string.Format("{0} years ago", Math.Round(timeSince.TotalDays / 365));
            return timeAgo;
        }

        public static byte[] HexToBytes(string hex)
        {
            int NumberChars = hex.Length;
            byte[] bytes = new byte[NumberChars / 2];
            for (int i = 0; i < NumberChars; i += 2)
            {
                bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
            }
            return bytes;
        }

        public static bool IsAnonymousType(Type type)
        {
            return type.FullName.StartsWith("<>f");
        }

        public static bool IsArrayType(Type type)
        {
            return type.Name.Contains("[]") || type.FullName.StartsWith("System.Linq.Enumerable") || IsListType(type);
        }

        public static bool IsGenericType(Type type)
        {
            return !type.FullName.StartsWith("System") && !IsAnonymousType(type);
        }

        public static bool IsGenericListType(Type type)
        {
            //Check if a list is of custom generic class type, i.e. non System type
            return IsListType(type) && !type.FullName.Split("[[")[1].StartsWith("System");
        }

        public static bool IsListType(Type type)
        {
            //Full name property of a type can be:
            //System.Int32
            //System.Int64
            //System.Nullable`1[[System.Int32, ...
            //System.Object (dynamic or object)
            //Apple.User
            //System.Collections.Generic.List`1[[System.Nullable`1[[System.Int32, ...]]
            //System.Collections.Generic.List`1[[System.String, ...]]
            //System.Collections.Generic.List`1[[Apple.User, ...]]
            //System.Collections.Generic.List`1[[System.Object, ...]]
            return type.Name == "List`1";
        }

        public static bool IsIdType(Type type)
        {
            return type == typeof(int) || type == typeof(long) ||
                type == typeof(int?) || type == typeof(long?) || type == typeof(string);
        }

        public static bool IsNumberType(Type type)
        {
            return type == typeof(byte) || type == typeof(int) || type == typeof(long) ||
                type == typeof(float) || type == typeof(double) || type == typeof(decimal) ||
                type == typeof(BigInteger) || type == typeof(byte?) || type == typeof(int?) || type == typeof(long?) ||
                type == typeof(float?) || type == typeof(double?) || type == typeof(decimal?) ||
                type == typeof(BigInteger?);
        }

        public static bool IsStringType(Type type)
        {
            return type == typeof(char) || type == typeof(string) || type == typeof(char?);
        }
        public static bool IsVoid(dynamic d)
        {
            //null, empty object {}, empty char/string, false, zero numeric value, or empty list
            if (d == null)
            {
                return true;
            }
            else if (d.GetType() == typeof(bool))
            {
                return !d;
            }
            else if (d.GetType() == typeof(char) || d.GetType() == typeof(string))
            {
                return d.ToString().Trim() == "";
            }
            else if (d.GetType() == typeof(byte) || d.GetType() == typeof(int) || d.GetType() == typeof(long) ||
                d.GetType() == typeof(float) ||d.GetType() == typeof(double) || d.GetType() == typeof(decimal) ||
                d.GetType() == typeof(BigInteger))
            {
                return d == 0;
            }
            else if (IsListType(d.GetType()))
            {
                return d.Count == 0;
            }
            else
            {
                return JSON.Serialize(d) == "{}" || JSON.Serialize(d) == "[]";
            }
        }

        public static TimeSpan ParseTime(string hrMinSec = "0:0:0")
        {
            var timeStrArr = hrMinSec.Split(':');
            if (timeStrArr.Count() > 3)
            {
                throw new Exception("Invalid time string, format must be `hr:min:sec`");
            }
            var timeSpanArr = new int[] { 0, 0, 0 };
            for (int i = 0; i < timeStrArr.Count(); i++)
            {
                int.TryParse(timeStrArr.ElementAt(i), out timeSpanArr[i]);
            }
            return new TimeSpan(timeSpanArr[0], timeSpanArr[1], timeSpanArr[2]);
        }

        public static string ReadFile(string file)
        {
            if (File.Exists(file))
            {
                try
                {
                    using (FileStream fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        StringBuilder sb = new StringBuilder();
                        using (StreamReader sr = new StreamReader(fs))
                        {
                            while (!sr.EndOfStream)
                            {
                                sb.AppendLine(sr.ReadLine());
                            }
                        }
                        return sb.ToString();
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.ToString());
                }
            }
            else
            {
                return "";
            }
        }

        public static string ToSentenceCase(string str)
        {
            return Regex.Replace(str, "([A-Z]{1,2}|[0-9]+)", " $1").TrimStart();
        }

        public static DateTime ToUTCDateTime(int unixTimeStamp)
        {
            return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(unixTimeStamp);
        }

        public static string ValidFileName(string fileName, string replaceStr = "")
        {
            var regexSearch = new string(Path.GetInvalidFileNameChars()) + new string(Path.GetInvalidPathChars());
            var r = new Regex(string.Format("[{0}]", Regex.Escape(regexSearch)));
            return r.Replace(fileName, replaceStr);
        }

        public static string WriteFile(string file = null, string content = null, bool append = false, string splitFilePostfix = null, long splitSize = 1048576)
        {
            //Content will be splitted based on splitSize, default split size is 1MB
            //File name will be appended with splitFilePostfix and original file extension
            //If splitFilePostfix is not provided default will be underscore and UTC Date/Time, e.g.: mytestfile_yyyyMMddHHmmss.txt
            try
            {
                FileMode fm = FileMode.Create;
                if (append)
                {
                    fm = FileMode.Append;
                }
                using (FileStream fs = new FileStream(file, fm, FileAccess.Write, FileShare.ReadWrite))
                {
                    if (append)
                    {
                        if (fs.Length > splitSize)
                        {
                            fm = FileMode.Create;
                            if (string.IsNullOrEmpty(splitFilePostfix))
                            {
                                splitFilePostfix = DateTime.UtcNow.ToString("_yyyyMMddHHmmss");
                            }
                            File.Copy(file, Path.GetDirectoryName(file) + "/" + Path.GetFileNameWithoutExtension(file) +
                                splitFilePostfix + Path.GetExtension(file));
                        }
                        else
                        {
                            if (fs.Length > 0)
                            {
                                content = Environment.NewLine + content;
                            }
                        }
                    }
                    using (StreamWriter sw = new StreamWriter(fs))
                    {
                        sw.Write(content);
                    }
                }
            }
            catch
            {
                using (FileStream fs = new FileStream(file, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    using (StreamWriter sw = new StreamWriter(fs))
                    {
                        sw.Write(content);
                    }
                }
            }
            return content;
        }

        public static void WriteLog(string logText, string logFile = null)
        {
            //Default will write to  AppDomain.CurrentDomain.BaseDirectory + @"log/log.txt" OR provide full file path
            string logFolder;
            if (logFile == null)
            {
                logFile = AppDomain.CurrentDomain.BaseDirectory + @"/log/log.txt";                           
            }
            logFolder = Path.GetDirectoryName(logFile);
            if (!Directory.Exists(logFolder))
            {
                Directory.CreateDirectory(logFolder);
            }
            WriteFile(logFile, logText, true);
        }
    }
}
