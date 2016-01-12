using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Data;

namespace MyDataBaseFramework
{
    public class BaseData
    {
        private string _name;
        private int _size;
        private int _filegrowth;
        private string _filepath;
        private string _dataSource;
        bool _isPhysical;

        public string Name
        {
            get { return _name; }
            private set { _name = value; }
        }

        public string DataSource
        {
            get { return _dataSource; }
            private set
            {
                _dataSource = value ?? "localhost";
            }
        }
        public string FilePath
        {
            get { return _filepath; }
            private set
            {
                if (!Directory.Exists(@"../../DATA"))
                    Directory.CreateDirectory(@"../../DATA");
                _filepath = value ?? String.Format(@"{0}/{1}.mdf", Path.GetFullPath(@"../../DATA"), this.Name);
            }
        }

        public int Size
        {
            get { return _size; }
            private set 
            {
                if (value > 10)
                    _size = value;
                else
                    _size = 10;
            }
        }

        public int FileGrowth
        {
            get { return _filegrowth; }
            private set
            {
                if (value > 5)
                    _filegrowth = value;
                else
                    _filegrowth = 5;
            }
        }

        public bool IsPhysical
        {
            get 
            {
                VerifyPhysicalModel();
                return _isPhysical; 
            }
            private set { _isPhysical = value; }
        }
        public BaseData(string name, int size = 10, int fileGrowth = 5, string dataSource = null, string filePath = null)
        {
            Name = name;
            DataSource = dataSource;
            Size = size;
            FileGrowth = fileGrowth;
            FilePath = filePath;
            VerifyPhysicalModel();
        }

        private void VerifyPhysicalModel()
        {
            SqlConnection connection;
            try
            {
                using (connection = ConnectToDataBase("master", this.DataSource))
                {
                    string SQLcommand = String.Format("SELECT name FROM sys.databases");
                    SqlCommand command = new SqlCommand(SQLcommand, connection);
                    using(SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (reader.GetSqlValue(0).ToString() == this.Name)
                            {
                                this.IsPhysical = true;
                                break;
                            }
                            else
                                this.IsPhysical = false;
                        }
                    }
                }
            }
            catch(Exception)
            {
                this.IsPhysical = false;
            }
        }

        public static BaseData CreateDataBase(string name, int size = 10, int filegrowth = 5, string datasourse = null, string filepath = null)
        {
            BaseData dataBase = new BaseData(name, size, filegrowth, datasourse, filepath);
            dataBase.CreatePhysicalModel(); 
            return dataBase;
        }

        public void CreatePhysicalModel()
        {
            if (this.IsPhysical == true)
                return;
            using (SqlConnection connection = ConnectToDataBase("master", this.DataSource))
            {
                try
                {
                    string SQLcommand = String.Format("CREATE DATABASE {0} ON(NAME = '{0}', FILENAME = '{1}', SIZE = {2}, FILEGROWTH = {3})",
                                                     this.Name, this.FilePath, this.Size, this.FileGrowth);
                    SqlCommand command = new SqlCommand(SQLcommand, connection);
                    command.ExecuteNonQuery();
                    VerifyPhysicalModel();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        public static SqlConnection ConnectToDataBase(string dataBaseName, string datasourse = null)
        {
            SqlConnectionStringBuilder connectionStrBuilder = InitialiseConnectionStrBuilder(dataBaseName, datasourse);
            SqlConnection connection = new SqlConnection(connectionStrBuilder.ConnectionString);
            connection.Open();
            return connection;
        }

        private static SqlConnectionStringBuilder InitialiseConnectionStrBuilder(string dataBaseName, string datasource = null)
        {
            SqlConnectionStringBuilder connectionStrBuilder = new SqlConnectionStringBuilder();
            connectionStrBuilder.DataSource = datasource ?? "localhost";
            connectionStrBuilder.InitialCatalog = dataBaseName;
            connectionStrBuilder.IntegratedSecurity = true;
            connectionStrBuilder.Pooling = true;
            return connectionStrBuilder;
        }

        public static void DropDatabase(string databaseName)
        {
            BaseData database = new BaseData(databaseName);
            database.Drop();
        }

        public void Drop()
        {
            if (this.IsPhysical == false)
                return;
            using (SqlConnection connection = ConnectToDataBase("master", this.DataSource))
            {
                try
                {
                    GoToSingleUserMode(connection);
                    DropDB(connection);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private void DropDB(SqlConnection connection)
        {
            string SQLcommandToDrop = String.Format("DROP DATABASE {0}", this.Name);
            SqlCommand commandDrop = new SqlCommand(SQLcommandToDrop, connection);
            commandDrop.ExecuteNonQuery();
        }

        private void GoToSingleUserMode(SqlConnection connection)
        {
            string SQLcommandToAlter = String.Format("ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE", this.Name);
            SqlCommand commandAlter = new SqlCommand(SQLcommandToAlter, connection);
            commandAlter.ExecuteNonQuery();
        }

        public TableData GetTable(string tableName)
        {
            if (this.IsPhysical == false)
                throw new Exception("Cannot open database");

            List<ColumnData> columns = new List<ColumnData>();
            bool isPrimary = false;
            bool isNull;
            ForeignKey foreignKey = null;

            DataTable tableColumnInfo;
            DataTable tableColumns;

            using (SqlConnection connection = ConnectToDataBase(this.Name, this.DataSource))
            {
                try
                {
                    string strCommandGetColumnsInfo = String.Format("SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{0}'", tableName);
                    SqlCommand commandGetColumnsInfo = new SqlCommand(strCommandGetColumnsInfo, connection);
                    using (SqlDataReader readerColumnsInfo = commandGetColumnsInfo.ExecuteReader())
                    {
                        tableColumnInfo = new DataTable();
                        tableColumnInfo.Load(readerColumnsInfo);
                    }
                    string strCommandGetColumns = String.Format("SELECT * FROM [{0}]", tableName);
                    SqlCommand commandGetColumns = new SqlCommand(strCommandGetColumns, connection);
                    using (SqlDataReader readerColumns = commandGetColumns.ExecuteReader())
                    {
                        tableColumns = new DataTable();
                        tableColumns.Load(readerColumns);
                    }
                    var cols = tableColumns.Columns;

                    for (int i = 0; i < tableColumnInfo.Rows.Count; i++)
                    {
                        string name = tableColumnInfo.Rows[i][0].ToString();
                        SqlDbType type = ConvertFromStr(tableColumnInfo.Rows[i][1].ToString());
                        int maxlength = (tableColumnInfo.Rows[i][2].ToString() != "") ? Int16.Parse(tableColumnInfo.Rows[i][2].ToString()) : 0;
                        if (tableColumnInfo.Rows[i][3].ToString() == "NO")
                            isNull = false;
                        else
                            isNull = true;
                        bool identity = cols[i].AutoIncrement;
                        bool isUnique = cols[i].Unique;
                        string strCommandGetColumnKeys = String.Format("SELECT CONSTRAINT_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{0}' AND COLUMN_NAME = '{1}'", tableName, name);
                        SqlCommand commandGetColumnsKeys = new SqlCommand(strCommandGetColumnKeys, connection);
                        using (SqlDataReader readerColumnsKeys = commandGetColumnsKeys.ExecuteReader())
                        {
                            while (readerColumnsKeys.Read())
                            {
                                string constraintName = readerColumnsKeys[0].ToString();
                                string[] parts = constraintName.Split(new char[] { '_' }, StringSplitOptions.RemoveEmptyEntries);
                                if (parts[0] == "FK")
                                    foreignKey = new ForeignKey(parts[2], parts[1]);
                                else if (parts[0] == "PK")
                                    isPrimary = true;
                            }
                        }
                        columns.Add(new ColumnData(name, type, maxlength, isNull, isPrimary, identity, foreignKey, isUnique));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                return TableData.CreateNew(tableName, this, columns);
            }
        }

        private SqlDbType ConvertFromStr(string type)
        {
            SqlDbType result = default(SqlDbType);
            switch(type.ToLower())
            {
                case "bigint":
                    result = SqlDbType.BigInt;
                    break;
                case "binary":
                    result = SqlDbType.Binary;
                    break;
                case "bit":
                    result = SqlDbType.Bit;
                    break;
                case "char":
                    result = SqlDbType.Char;
                    break;
                case "date":
                    result = SqlDbType.Date;
                    break;
                case "datetime":
                    result = SqlDbType.DateTime;
                    break;
                case "datetime2":
                    result = SqlDbType.DateTime2;
                    break;
                case "datetimeoffset":
                    result = SqlDbType.DateTimeOffset;
                    break;
                case "decimal":
                    result = SqlDbType.Decimal;
                    break;
                case "float":
                    result = SqlDbType.Float;
                    break;
                case "image":
                    result = SqlDbType.Image;
                    break;
                case "int":
                    result = SqlDbType.Int;
                    break;
                case "money":
                    result = SqlDbType.Money;
                    break;
                case "nchar":
                    result = SqlDbType.NChar;
                    break;
                case "ntext":
                    result = SqlDbType.NText;
                    break;
                case "nvarchar":
                    result = SqlDbType.NVarChar;
                    break;
                case "real":
                    result = SqlDbType.Real;
                    break;
                case "smalldatetime":
                    result = SqlDbType.SmallDateTime;
                    break;
                case "smallint":
                    result = SqlDbType.SmallInt;
                    break;
                case "smallmoney":
                    result = SqlDbType.SmallMoney;
                    break;
                case "structured":
                    result = SqlDbType.Structured;
                    break;
                case "text":
                    result = SqlDbType.Text;
                    break;
                case "time":
                    result = SqlDbType.Time;
                    break;
                case "timestamp":
                    result = SqlDbType.Timestamp;
                    break;
                case "tinyint":
                    result = SqlDbType.TinyInt;
                    break;
                case "udt":
                    result = SqlDbType.Udt;
                    break;
                case "uniqueidentifier":
                    result = SqlDbType.UniqueIdentifier;
                    break;
                case "varbinary":
                    result = SqlDbType.VarBinary;
                    break;
                case "varchar":
                    result = SqlDbType.VarChar;
                    break;
                case "variant":
                    result = SqlDbType.Variant;
                    break;
                case "xml":
                    result = SqlDbType.Xml;
                    break;
            }
            return result;
        }
    }
}
